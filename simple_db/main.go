package main

import (
	bmg "buffer_manager"
	fm "file_manager"
	lm "log_manager"
	"tx"
)

func main() {
	file_manager, _ := fm.NewFileManager("txtest", 400)
	log_manager, _ := lm.NewLogManager(file_manager, "logfile")
	buffer_manager := bmg.NewBufferManager(file_manager, log_manager, 3)

	txA := tx.NewTransation(file_manager, log_manager, buffer_manager)
	txB := tx.NewTransation(file_manager, log_manager, buffer_manager)
	blk1 := fm.NewBlockId("testfile", 1)
	blk11 := fm.NewBlockId("testfile", 1)
	txA.SetInt(blk1, 0, 0, false)
	txB.GetInt(blk11, 0)
}
