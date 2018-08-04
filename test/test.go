package main

import (
	"fmt"
	"myDHT/cli"
	"strconv"
	"time"
)

var cnt int
var cho [56]dhtchord.Chord
var tmp int

func test() {
	//test1
	m := make(map[string]string)

	cho[0].CreateCommand()

	for loop := 0; loop < 5; loop++ {
		for i := 1; i <= 15; i++ {
			tmp = i + loop*10
			port := 3410 + tmp
			p := strconv.Itoa(port)
			cho[tmp].PortCommand(p)
			cho[tmp].JoinCommand(cho[0].Localaddr())
			time.Sleep(1 * time.Second)
		}

		for i := 0; i < 300; i++ {
			key := dhtchord.GetRandomString(20)
			value := dhtchord.GetRandomString(5)
			cho[0].PutCommand(key, value)
			m[key] = value
		}

		for i := 0; i <= tmp; i++ {
			cho[i].DumpCommand()
		}
		fmt.Println('\n')

		num := 0
		for key, val := range m {
			value, _ := cho[0].GetCommand(key)
			if value != val {
				k := 0
				for ; k < 5; k++ {
					time.Sleep(500 * time.Millisecond)
					value, _ = cho[0].GetCommand(key)
					if value == val {
						break
					}
				}
				if k == 5 {
					fmt.Println(value, " ", key, "cannot find")
					cnt++
				}
			}
			num++
			if num == 200 {
				break
			}

		}
		num = 0
		for key := range m {
			cho[0].DeleteCommand(key)
			delete(m, key)
			num++
			if num == 150 {
				break
			}
		}

		for i := 11; i <= 15; i++ {
			tmp = i + loop*10
			cho[tmp].QuitCommand()
			time.Sleep(1 * time.Second)
		}

		for i := 0; i < 300; i++ {
			key := dhtchord.GetRandomString(20)
			value := dhtchord.GetRandomString(5)
			cho[0].PutCommand(key, value)
			m[key] = value
		}

		for i := 0; i <= tmp-5; i++ {
			cho[i].DumpCommand()
		}
		fmt.Println('\n')

		num = 0
		for key, val := range m {
			value, _ := cho[0].GetCommand(key)
			if value != val {
				k := 0
				for ; k < 5; k++ {
					time.Sleep(500 * time.Millisecond)
					value, _ = cho[0].GetCommand(key)
					if value == val {
						break
					}
				}
				if k == 5 {
					fmt.Println(value, " ", key, "cannot find")
					cnt++
				}
			}
			num++
			if num == 200 {
				break
			}

		}
		num = 0
		for key := range m {
			cho[0].DeleteCommand(key)
			delete(m, key)
			num++
			if num == 150 {
				break
			}
		}

	}
	if cnt == 0 {
		fmt.Printf("\nPass!\n")
	} else {
		fmt.Printf("mistakes: %v, fail", cnt)
	}

}

func main() {
	test()
}
