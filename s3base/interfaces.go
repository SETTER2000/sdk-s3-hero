package s3base

import "github.com/aws/aws-sdk-go-v2/service/s3/types"

type S3Basics interface {
	ListBuckets() ([]types.Bucket, error)
	BucketExists(bucketName string) (bool, error)
	CreateBucket(name string, region string) error
	UploadFile(bucketName string, objectKey string, fileName string) error
	UploadLargeObject(bucketName string, objectKey string, largeObject []byte) error
	DownloadFile(bucketName string, objectKey string, fileName string) error
	DownloadLargeObject(bucketName string, objectKey string) ([]byte, error)
	CopyToFolder(bucketName string, objectKey string, folderName string) error
	ListObjects(bucketName string) ([]types.Object, error) //Put(*BucketBasics) error
	DeleteObjects(bucketName string, objectKeys []string) error
	DeleteBucket(bucketName string) error
	GetListFirstPage()
	//Get(key string) (*BucketBasics, error)
	//GetAll(*BucketBasics) (*BucketBasics, error)
}
