package operation

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"os"
	"restic-glacier/internal/shared"
)

var Freeze = &opFreeze{}

type opFreeze struct {
}

func (o opFreeze) FilterStorageClasses() []string {
	return []string{s3.StorageClassOnezoneIa, s3.StorageClassStandard}
}

func (o opFreeze) OperateOnObject(ctx context.Context, svc *s3.S3, bucket string, object s3.Object) {
	shared.Ratelimit.Take()
	_, err := svc.CopyObjectWithContext(ctx, &s3.CopyObjectInput{
		StorageClass: aws.String(s3.StorageClassGlacier),
		CopySource:   aws.String(fmt.Sprintf("%s/%s", bucket, *object.Key)),
		Bucket:       aws.String(bucket),
		Key:          object.Key,
	})
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
}

func (o opFreeze) WaitForCompletion() {
	//no-op
}
