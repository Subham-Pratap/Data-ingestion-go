package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go/aws"
)

func ingestLogsHandler(w http.ResponseWriter, r *http.Request) {
	// Parse the incoming JSON data
	var logEntries []LogE
	if err := json.NewDecoder(r.Body).Decode(&logEntries); err != nil {
		http.Error(w, fmt.Sprintf("Failed to decode JSON: %v", err), http.StatusBadRequest)
		return
	}

	// Sort the log entries by timestamp
	sort.Slice(logEntries, func(i, j int) bool {
		return logEntries[i].Time < logEntries[j].Time
	})

	// Initializing NEW AWS S3 client TO LOAD THE CONFIG
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(region))
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to load AWS config: %v", err), http.StatusInternalServerError)
		return
	}

	s3Client := s3.NewFromConfig(cfg)

	// SAVING THE SORTED LOGS TO S3
	objectKeyPrefix := "subhampratap" + time.Now().Format("2006-01-02T15:04:05/")
	for i, entry := range logEntries {
		objectKey := fmt.Sprintf("%slog%d.json", objectKeyPrefix, i)

		// ConvertING the log entry to JSON FORMAT
		logJSON, err := json.Marshal(entry)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to marshal JSON: %v", err), http.StatusInternalServerError)
			return
		}

		// UploadING the log entry to S3
		_, err = s3Client.PutObject(context.TODO(), &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
			Body:   aws.ReadSeekCloser(bytes.NewReader(logJSON)),
		})

		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to upload log entry to S3: %v", err), http.StatusInternalServerError)
			return
		}
	}

	// Respond with success
	w.WriteHeader(http.StatusOK)
}
