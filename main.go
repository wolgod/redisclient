package main

import (
     "fmt"
	"redisclient/utils"
)



func main(){
	test1()
}

func test1()  {
	utils.RedisSet("age",18,0)
	get, err := utils.RedisGet("age")
	if err !=nil {
		fmt.Println("err:",err)
		return
	}
	fmt.Println(get)
}