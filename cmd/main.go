package main

import (
	"context"
	"github.com/SETTER2000/sdk-s3-hero/s3base"
	"github.com/aws/aws-sdk-go-v2/config"
	"log"
)

func main() {
	var s3baseUseCase s3base.S3Basics
	// Load the Shared AWS Configuration (~/.aws/config)
	// Загрузить общую конфигурацию AWS (~/.aws/config)
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatal(err)
	}
	s := s3base.New(s3baseUseCase, cfg)
	//storage.UploadFile(bucketName, objectKey, fileName)
	s.ListBuckets()
}
