package cmd

import (
	"github.com/spf13/cobra"
	"restic-glacier/internal/operation"
	"restic-glacier/internal/storage"
)

func init() {
	rootCmd.AddCommand(moveToGlacierCmd)
}

var moveToGlacierCmd = &cobra.Command{
	Use:   "move-to-glacier",
	Short: "Move data/, index/ and snapshots/ to Glacier",
	Run: func(cmd *cobra.Command, args []string) {
		storage.OperateOnData(cmd.Context(), operation.Freeze)
	},
}
