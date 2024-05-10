FROM icr.io/codeengine/golang:alpine
COPY health-checker.go /
COPY go.mod /
COPY go.sum /
RUN  go get github.com/IBM/cloudant-go-sdk/cloudantv1@v0.7.6
RUN  go build -o /health-checker /health-checker.go

# Copy the exe into a smaller base image
FROM icr.io/codeengine/alpine
COPY --from=0 /health-checker /health-checker
CMD  /health-checker