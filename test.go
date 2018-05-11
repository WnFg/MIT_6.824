package main

import "fmt"

func main() {

	messages := make(chan string)

	messages <- "buffered"
	//	messages <- "channel"

	//	fmt.Println(<-messages)
	fmt.Println(<-messages)
}

//fmt.Println("vim-go")
