package storage

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"os"
	"restic-glacier/internal/operation"
	"restic-glacier/internal/shared"
	"sync"
)

const NumMoveWorker = 100

func OperateOnData(ctx context.Context, op operation.Operation) {
	var bucket = os.Getenv("BUCKET_NAME")

	cfg := &aws.Config{}
	if endpoint, ok := os.LookupEnv("AWS_ENDPOINT"); ok {
		cfg.Endpoint = &endpoint
	}

	sess, err := session.NewSession(cfg)
	if err != nil {
		shared.Logger.Errorw("Cannot create session", "error", err)
		os.Exit(1)
	}

	svc := s3.New(sess)
	output := make(chan *s3.Object, 1000)

	go func() {
		shared.Logger.Info("Listing objects requiring changes...")
		listObjectsInStorageClass(ctx, svc, bucket, "data/", op.FilterStorageClasses(), output)
		listObjectsInStorageClass(ctx, svc, bucket, "index/", op.FilterStorageClasses(), output)
		listObjectsInStorageClass(ctx, svc, bucket, "snapshots/", op.FilterStorageClasses(), output)

		shared.Logger.Info("Done listing objects")
		close(output)
	}()

	var w sync.WaitGroup
	w.Add(NumMoveWorker)
	for i := 1; i <= NumMoveWorker; i++ {
		go func(i int, ci <-chan *s3.Object) {
			for object := range ci {
				shared.Logger.Infof("[%03d] Moving %s from %s..", i, *object.Key, *object.StorageClass)
				op.OperateOnObject(ctx, svc, bucket, *object)
			}
			w.Done()
		}(i, output)
	}

	w.Wait()

	shared.Logger.Info("Waiting for completion of operation...")

	op.WaitForCompletion()
}

func listObjectsInStorageClass(ctx context.Context, svc *s3.S3, bucket string, prefix string, storageClasses []string, output chan<- *s3.Object) {
	input := &s3.ListObjectsV2Input{
		Bucket:  aws.String(bucket),
		Prefix:  aws.String(prefix),
		MaxKeys: aws.Int64(5000),
	}
	for {
		shared.Ratelimit.Take()
		result, err := svc.ListObjectsV2WithContext(ctx, input)
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok && aerr.Code() == request.CanceledErrorCode {
				// If the SDK can determine the request or retry delay was canceled
				// by a context the CanceledErrorCode error code will be returned.
				fmt.Fprintf(os.Stderr, "upload canceled due to timeout, %v\n", err)
			} else {
				fmt.Fprintf(os.Stderr, "failed to upload object, %v\n", err)
			}
			os.Exit(1)
		}

		for _, content := range result.Contents {
			for _, class := range storageClasses {
				if *content.StorageClass == class {
					output <- content
					break
				}
			}
		}

		if !*result.IsTruncated {
			break
		}

		input.SetContinuationToken(*result.NextContinuationToken)
	}
}
