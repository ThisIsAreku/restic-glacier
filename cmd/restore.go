package cmd

import (
	"github.com/spf13/cobra"
	"restic-glacier/internal/operation"
	"restic-glacier/internal/storage"
)

func init() {
	rootCmd.AddCommand(restoreCmd)
}

var restoreCmd = &cobra.Command{
	Use:   "restore",
	Short: "Restore data/, index/ and snapshots/ back from Glacier to ONEZONE_IA ",
	Run: func(cmd *cobra.Command, args []string) {
		storage.OperateOnData(cmd.Context(), operation.Restore)
	},
}
