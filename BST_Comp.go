package main

import(
	"flag"
	"fmt"
	"bufio"
	"os"
	"strings"
	"strconv"
)
// A Tree is a binary tree with integer values.
type Tree struct {
	Left  *Tree
	Value int
	Right *Tree
}
// Walk traverses a tree depth-first,
// sending each Value on a channel.
func Walk(t *Tree, ch chan int) {
	if t == nil {
		return
	}
	Walk(t.Left, ch)
	ch <- t.Value
	Walk(t.Right, ch)
}


// Walker launches Walk in a new goroutine,
// and returns a read-only channel of values.
func Walker(t *Tree) <-chan int {
	ch := make(chan int)
	go func() {
		Walk(t, ch)
		close(ch)
	}()
	return ch
}

func insert(t *Tree, v int) *Tree {
	if t == nil {
		return &Tree{nil, v, nil}
	}
	if v < t.Value {
		t.Left = insert(t.Left, v)
		return t
	}
	t.Right = insert(t.Right, v)
	return t
}

// Compare reads values from two Walkers
// that run simultaneously, and returns true
// if t1 and t2 have the same contents.
func Compare(t1, t2 *Tree) bool {
	c1, c2 := Walker(t1), Walker(t2)
	for {
		v1, ok1 := <-c1
		v2, ok2 := <-c2
		if !ok1 || !ok2 {
			return ok1 == ok2
		}
		if v1 != v2 {
			break
		}
	}
	return false
}


func read_file(input string, nums *[][]int){
	fmt.Println("reading file: ", input)
	readFile, err := os.Open(input)

	if err!=nil{
		fmt.Println(err)
	}
	fileScanner := bufio.NewScanner(readFile)
	fileScanner.Split(bufio.ScanLines)

	for fileScanner.Scan(){
		line := fileScanner.Text()
		numbers := strings.Split(line, " ")
		var temp []int

		for _, ch := range numbers{
			num, _ := strconv.Atoi(ch)
			temp = append(temp, num)
			//fmt.Print(temp, " ")
		}
		*nums = append(*nums, temp)
	}

	readFile.Close()
}
func new_tree(num []int) *Tree{
	var tree *Tree
	for _, n := range num{
		tree = insert(tree, n)
	}
	return tree
}

func main(){
	hash_workers := flag.Int("hash-workers", 1, "number of threads to do hash computation")
	data_workers := flag.Int("data-workers", 0, "number of threads to update the map")
	comp_workers := flag.Int("comp-workers", 0, "number of threads to do comparison")
	input := flag.String("input", "", "path to an input file")

	flag.Parse()

	if *input ==""{
		fmt.Println("Please specify path to an input file e.g. -input .\\simple.txt")
		return
	}

	var nums [][]int

	
	fmt.Println("hash-workers", *hash_workers)
	fmt.Println("data-workers", *data_workers) 
	fmt.Println("comp-workers", *comp_workers)
	fmt.Println("input", *input)

	read_file(*input, &nums)
/*	for _, line := range nums{
		for _, num := range line{
			fmt.Print(num, " ")
		}
		fmt.Print("\n")
	}*/
	
	var trees []*Tree

	for _, num := range nums{
		t := new_tree(num)
		trees = append(trees, t)
	}
	

	ch := Walker(trees[11])
	for{
		v1, ok1 := <-ch
		
		if !ok1 {
			fmt.Print("\nend")
			break
		}
		fmt.Print(v1," ")
	}
	
}