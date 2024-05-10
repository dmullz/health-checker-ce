FROM icr.io/codeengine/golang:alpine
COPY health-checker.go /
RUN  go build -o /health-checker /health-checker.go

# Copy the exe into a smaller base image
FROM icr.io/codeengine/alpine
COPY --from=0 /health-checker /health-checker
CMD  /health-checker