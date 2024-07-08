package main

import (
	"bytes"
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/IBM/cloudant-go-sdk/cloudantv1"
)

type RssFeed struct {
	Id              string  `json:"_id"`
	RssFeedName     string  `json:"RSS_Feed_Name"`
	RssFeedUrl      string  `json:"RSS_Feed_URL"`
	LastUpdatedDate string  `json:"Last_Updated_Date"`
	Threshold       float32 `json:"Threshold"`
	Magazine        string  `json:"Magazine"`
}

type Feed struct {
	Publisher       string `json:"publisher"`
	FeedUrl         string `json:"feed_url"`
	LastUpdatedDate string `json:"last_updated_date"`
	FeedName        string `json:"feed_name"`
}

type DBRow struct {
	Id               string  `json:"id"`
	ArticleTitle     string  `json:"article_title"`
	ArticlePublisher string  `json:"article_publisher"`
	ArticleMagazine  string  `json:"article_magazine"`
	ArticleUrl       string  `json:"article_url"`
	ArticlePubdate   int64   `json:"article_pubdate"`
	LeadClassifier   float32 `json:"lead_classifier"`
	SentimentScore   float32 `json:"sentiment_score"`
}

type MagazineData struct {
	Magazine         string
	IngestedArticles int
}

type DBQuery struct {
	ApiKey     string `json:"apikey"`
	IngestDate string `json:"ingestdate"`
	Magazine   string `json:"magazine"`
}

type BrevoSender struct {
	Name  string `json:"sender"`
	Email string `json:"email"`
}

type BrevoTo struct {
	Email string `json:"email"`
}

type BrevoAttachment struct {
	Content string `json:"content"`
	Name    string `json:"name"`
}

type BrevoQuery struct {
	Sender      BrevoSender       `json:"sender"`
	To          []BrevoTo         `json:"to"`
	Subject     string            `json:"subject"`
	HtmlContent string            `json:"htmlContent"`
	Attachment  []BrevoAttachment `json:"attachment"`
}

func main() {

	// Get the namespace we're in so we know how to talk to the Function
	file := "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
	namespace, err := ioutil.ReadFile(file)
	if err != nil || len(namespace) == 0 {
		fmt.Fprintf(os.Stderr, "Missing namespace: %s\n%s\n", err, namespace)
		os.Exit(1)
	}

	// Query Cloudant for the feed list
	// selector= {"_id": {"$gt": "0"},"Publisher_Name": {"$exists": True},"RSS_Feeds": {"$exists": True}},
	service, err := cloudantv1.NewCloudantV1UsingExternalConfig(
		&cloudantv1.CloudantV1Options{},
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error initializing Cloudant Service: %s", err)
		os.Exit(1)
	}

	selector := map[string]interface{}{
		"_id": map[string]interface{}{
			"$gt": "0",
		},
		"Publisher_Name": map[string]interface{}{
			"$exists": true,
		},
		"RSS_Feeds": map[string]interface{}{
			"$exists": true,
		},
	}
	dbName := os.Getenv("db_name")
	queryOptions := &cloudantv1.PostFindOptions{
		Db:       &dbName,
		Selector: selector,
	}

	// Execute the query
	findResult, _, err := service.PostFind(queryOptions)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error Finding All Documents using Cloudant Service: %s", err)
		os.Exit(1)
	}

	// Parse Result from Cloudant to build slice of RSS Feeds
	var feeds []Feed
	for _, doc := range findResult.Docs {
		var rssFeeds []RssFeed
		b, err := json.Marshal(doc.GetProperty("RSS_Feeds"))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error Marshaling RSS_Feeds interface into JSON: %s", err)
			os.Exit(1)
		}
		err = json.Unmarshal(b, &rssFeeds)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error Decoding JSON: %s", err)
			os.Exit(1)
		}
		for _, rssfeed := range rssFeeds {
			feed := Feed{
				Publisher:       doc.GetProperty("Publisher_Name").(string),
				FeedUrl:         rssfeed.RssFeedUrl,
				FeedName:        rssfeed.RssFeedName,
				LastUpdatedDate: rssfeed.LastUpdatedDate,
			}
			feeds = append(feeds, feed)
		}
	}

	count := len(feeds)
	fmt.Printf("Getting articles ingested for %d feeds...\n", count)
	wg := sync.WaitGroup{}

	// URL to the DB
	url := os.Getenv("sql_db_url") + "v2/get-article-by-ingestdate-magazine"

	// IngestDate of 24 hours ago
	toAdd := -24 * time.Hour
	ingestDate := time.Now().UTC().Add(toAdd)

	// Create channel to store DB responses
	magDataCh := make(chan MagazineData, count)

	// Do all requests to the DB in parallel
	for i := 0; i < count; i++ {
		payload := DBQuery{
			ApiKey:     os.Getenv("sql_db_apikey"),
			IngestDate: ingestDate.Format("2006-1-2"),
			Magazine:   feeds[i].FeedName,
		}
		payloadJson, _ := json.Marshal(payload)
		wg.Add(1)
		go func(i int, payloadJson []byte, magazine string) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				res, err := http.Post(url, "application/json", bytes.NewBuffer(payloadJson))

				if err == nil && res.StatusCode/100 == 2 {
					var dbRes []DBRow
					err := json.NewDecoder(res.Body).Decode(&dbRes)
					if err != nil {
						fmt.Println("JSON decode for DB ROW error!")
						panic(err)
					}
					magData := MagazineData{
						Magazine:         magazine,
						IngestedArticles: len(dbRes),
					}
					magDataCh <- magData
					break
				}

				// Something went wrong, pause and try again
				body := []byte{}
				if res != nil {
					body, _ = ioutil.ReadAll(res.Body)
				}
				fmt.Fprintf(os.Stderr, "%d: err: %s\nhttp res: %#v\nbody:%s",
					i, err, res, string(body))
				time.Sleep(time.Second)
			}
		}(i, payloadJson, feeds[i].FeedName)
	}

	// Wait for all threads to finish before we exit
	wg.Wait()
	close(magDataCh)

	// Gather Data From Channel
	allMagData := make(map[string]int)
	for chValue := range magDataCh {
		allMagData[chValue.Magazine] = chValue.IngestedArticles
	}

	// Sort results before building CSV
	keys := make([]string, 0, len(allMagData))
	for key := range allMagData {
		keys = append(keys, key)
	}
	sort.SliceStable(keys, func(i, j int) bool {
		return allMagData[keys[i]] < allMagData[keys[j]]
	})

	//Build CSV file with article data
	csvFile, err := os.Create("daily_article_data.csv")
	defer csvFile.Close()
	if err != nil {
		fmt.Printf("failed creating file: %s", err)
		panic(err)
	}

	w := csv.NewWriter(csvFile)
	defer w.Flush()

	w.Write([]string{"magazine", "articles"})
	for _, key := range keys {
		row := []string{key, strconv.Itoa(allMagData[key])}
		if err := w.Write(row); err != nil {
			fmt.Printf("Failed to write magazine to file: %s", err)
			panic(err)
		}
	}

	//Convert CSV file to base64 to attach to email
	fileBytes, err := os.ReadFile("daily_article_data.csv")
	if err != nil {
		fmt.Printf("Error reading csv file: %s", err)
		panic(err)
	}
	fileContent := base64.StdEncoding.EncodeToString(fileBytes)

	//Send CSV file in email using brevo
	todayDate := time.Now()
	todayString := todayDate.Format("2006-1-2")
	fileName := "daily_article_data_" + todayString + ".csv"
	client := &http.Client{}
	var toList []BrevoTo
	toList = append(toList, BrevoTo{Email: "david.mullen.085@gmail.com"})
	toList = append(toList, BrevoTo{Email: os.Getenv("email_address")})
	var attachmentList []BrevoAttachment
	attachmentList = append(attachmentList, BrevoAttachment{Content: fileContent, Name: fileName})
	payload := BrevoQuery{
		Sender: BrevoSender{
			Name:  "RSS Mailer",
			Email: "WM.RSS.mailer@gmail.com",
		},
		To:          toList,
		Subject:     "RSS Feed Health Status",
		HtmlContent: "<html><head></head><body>See attached for the total ingested articles in the past 24 hours by magazine.</body></html>",
		Attachment:  attachmentList,
	}
	payloadJson, _ := json.Marshal(payload)
	req, err := http.NewRequest("POST", "https://api.brevo.com/v3/smtp/email", bytes.NewBuffer(payloadJson))
	if err != nil {
		fmt.Printf("Error creating HTTP request to Brevo: %s", err)
		panic(err)
	}
	req.Header.Set("api-key", os.Getenv("brevo_api_key"))

	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("Error:", err)
		panic(err)
	}
	defer resp.Body.Close()

	//Remove CSV file
	err = os.Remove("daily_article_data.csv")
	if err != nil {
		fmt.Printf("Failed to delete article data file: %s", err)
		panic(err)
	}

	fmt.Printf("Done\n")

}
