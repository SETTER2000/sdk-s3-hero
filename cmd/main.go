package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"log"
	"sdk-s3-hero/s3base"
)

const bucketName = "paltos"

func main() {
	var s3baseUseCase s3base.S3Basics

	// Load the Shared AWS Configuration (~/.aws/config)
	// Загрузить общую конфигурацию AWS (~/.aws/config)
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatal(err)
	}
	s := s3base.New(s3baseUseCase, cfg)

	//err = s.UploadUpdateFile(bucketName, "1.jpg", "i.jpg")
	//err = s.DownloadFile(bucketName, "00120fa4-c964-4cc5-95b0-93ba33efdff0.jpg", "1.jpg")
	output, err := s.GetListFirstPage(bucketName)
	if err != nil {
		fmt.Printf("Err: %e", err)
	}
	log.Println("first page results:")
	for _, object := range output.Contents {
		log.Printf("key=%s size=%d", aws.ToString(object.Key), object.Size)
	}
	//err = s.CreateBucket("assperationius", "us-east-1")

	fmt.Printf("Ok!:: %v", err)

	//o, err := s.BucketExists("assperationius")
	//if err != nil {
	//	fmt.Printf("Err: %e", err)
	//}

	//fmt.Printf("D:: %v", o)

}
