package operation

import (
	"context"
	"github.com/aws/aws-sdk-go/service/s3"
)

type Operation interface {
	FilterStorageClasses() []string
	OperateOnObject(ctx context.Context, svc *s3.S3, bucket string, object s3.Object)
	WaitForCompletion()
}
