package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// LogEntry type
type LogE struct {
	Time int64  `json:"time"`
	Log  string `json:"log"`
}

// Define the S3 bucket name and region
const (
	bucketName = "mw-code-tester" // Replace with your S3 bucket name
	region     = "ap-south-1"     // Replace with your AWS region
)

// Initialize AWS S3 client
var s3Client *s3.Client

func init() {

	// Hardcoded the credentials for simplicity purposes
	awsAccessKeyID := os.Getenv("AWS_ACCESS_KEY_ID")
	awsSecretAccessKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	awsRegion := os.Getenv("AWS_REGION")

	//creating a connection config
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(awsRegion),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(awsAccessKeyID, awsSecretAccessKey, "")),
	)
	if err != nil {
		panic(fmt.Errorf("failed to load AWS config: %v", err))
	}

	s3Client = s3.NewFromConfig(cfg)
}

//HANDLER FUNCTION WHICH FETCHES/DOWNLOADS THE DATA FROM THE S3 CONNECTION

func searchLogs(w http.ResponseWriter, r *http.Request) {

	//GETTING THE QUERY PARAMETERS FROM THE REQUEST URL
	startTimeStr := r.URL.Query().Get("start")
	endTimeStr := r.URL.Query().Get("end")
	searchText := r.URL.Query().Get("text")

	//CONVERTING THE UNIX TIME INTO INTEGER
	startTime, err := strconv.ParseInt(startTimeStr, 10, 64)
	if err != nil {
		http.Error(w, "Invalid start time", http.StatusBadRequest)
		return
	}

	endTime, err := strconv.ParseInt(endTimeStr, 10, 64)
	if err != nil {
		http.Error(w, "Invalid end time", http.StatusBadRequest)
		return
	}

	//
	listInput := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	}

	listOutput, err := s3Client.ListObjectsV2(context.TODO(), listInput)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to list S3 objects: %v", err), http.StatusInternalServerError)
		return
	}

	// PROCESSING THE OUTPUT FROM S3
	results := []LogE{} // Create a slice to hold LogEntry objects

	for _, object := range listOutput.Contents {
		getInput := &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    object.Key,
		}

		getOutput, err := s3Client.GetObject(context.TODO(), getInput)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to get S3 object: %v", err), http.StatusInternalServerError)
			return
		}

		body, err := ioutil.ReadAll(getOutput.Body)
		getOutput.Body.Close()
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to read S3 object body: %v", err), http.StatusInternalServerError)
			return
		}

		var log LogE
		if err := json.Unmarshal(body, &log); err != nil {
			http.Error(w, fmt.Sprintf("Failed to unmarshal JSON: %v", err), http.StatusInternalServerError)
			return
		}

		// Now, the 'results' slice contains all the LogEntry objects

		if log.Time >= startTime && log.Time <= endTime && containsText(log.Log, searchText) {
			results = append(results, log)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(results)
}

func containsText(s, text string) bool {
	return text == "" || (s != "" && len(s) >= len(text) && s[:len(text)] == text)
}

func main() {
	http.HandleFunc("/query", searchLogs)         // to get the log data
	http.HandleFunc("/ingest", ingestLogsHandler) // to ingest the data to S3

	port := 8080
	fmt.Printf("Server listening on :%d\n", port)
	if err := http.ListenAndServe(fmt.Sprintf(":%d", port), nil); err != nil {
		log.Fatalf("Error starting server: %s\n", err)
	}
}
