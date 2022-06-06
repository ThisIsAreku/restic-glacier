package cmd

import (
	"fmt"
	"os"
	"restic-glacier/internal/shared"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use: "restic-glacier",
}

func Execute() {
	defer shared.Logger.Sync()
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.CompletionOptions.DisableDefaultCmd = true
}
