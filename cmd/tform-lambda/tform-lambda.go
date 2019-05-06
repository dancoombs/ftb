package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"log"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	"github.com/dancoombs/ftb/internal/awsjson"
	"github.com/dancoombs/ftb/internal/tform"
)

func handleRequest(ctx context.Context, createEvent awsjson.S3Event) (string, error) {
	log.Println("Lambda started from s3 create event")

	inBucket := createEvent.Records[0].S3.Bucket.Name
	inKey := createEvent.Records[0].S3.Object.Key

	outBucket := "ftb-transform"
	outKey := inKey

	log.Println(
		"In file", inBucket, "/", inKey,
		"Out file", outBucket, "/", outKey,
	)

	sess := session.Must(session.NewSession())
	writeBuf := aws.NewWriteAtBuffer([]byte{})

	log.Println("Downloading infile form s3")
	downloader := s3manager.NewDownloader(sess)
	n, err := downloader.Download(writeBuf, &s3.GetObjectInput{
		Bucket: aws.String(inBucket),
		Key:    aws.String(inKey),
	})
	if err != nil {
		return "Failure during s3 download", err
	}
	log.Printf("Sucessfully downloaded %d bytes", n)

	log.Printf("Transforming input file to ftb format")
	tformBuf := new(bytes.Buffer)
	csvWriter := csv.NewWriter(tformBuf)
	csvReader := csv.NewReader(bytes.NewReader(writeBuf.Bytes()))

	err = tform.DoTransform(csvReader, csvWriter, 1)
	if err != nil {
		return "Failure during s3 transform", err
	}
	log.Println("Sucessfully transformed infile to ftb format")

	log.Println("Uploading transform output to s3")
	uploader := s3manager.NewUploader(sess)
	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(outBucket),
		Key:    aws.String(outKey),
		Body:   tformBuf,
	})
	if err != nil {
		return "Failure during s3 upload", err
	}
	log.Println("Sucessfully uploaded file to s3")

	return "Success", nil
}

func main() {
	lambda.Start(handleRequest)
}
