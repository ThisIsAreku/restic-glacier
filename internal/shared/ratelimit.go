package shared

import "go.uber.org/ratelimit"

var Ratelimit = ratelimit.New(200)
