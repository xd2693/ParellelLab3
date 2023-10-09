package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
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

func CompareYaojie(t1, t2 *Tree, level int) bool {
	if t1 == nil && t2 == nil {
		return true
	}
	if (t1 == nil && t2 != nil) || (t1 != nil && t2 == nil) {
		return false
	}
	fmt.Printf("Level %d left %d right %d\n", level, t1.Value, t2.Value)
	return CompareYaojie(t1.Left, t2.Left, level+1) && (t1.Value == t2.Value) && CompareYaojie(t1.Right, t2.Right, level+1)
}

// read file from input into array nums [][]
func read_file(input string, nums *[][]int) {
	//fmt.Println("reading file: ", input)
	readFile, err := os.Open(input)

	if err != nil {
		fmt.Println(err)
	}
	fileScanner := bufio.NewScanner(readFile)
	fileScanner.Split(bufio.ScanLines)

	for fileScanner.Scan() {
		line := fileScanner.Text()
		numbers := strings.Split(line, " ")
		var temp []int

		for _, ch := range numbers {
			num, _ := strconv.Atoi(ch)
			temp = append(temp, num)
			//fmt.Print(temp, " ")
		}
		*nums = append(*nums, temp)
	}
	readFile.Close()
}

// construct one tree from array
func new_tree(num []int) *Tree {
	var tree *Tree
	for _, n := range num {
		tree = insert(tree, n)
	}
	return tree
}

func hash_work(tree *Tree) int {
	ch := Walker(tree)
	hash := 1
	new_value := 0
	for {
		value, ok := <-ch
		if !ok {
			//fmt.Print("\nend")
			break
		}
		new_value = value + 2
		hash = (hash*new_value + new_value) % 1000
	}
	return hash
}

/*
func central_manager(hash_workers int, data_workers int, nums [][]int, hash_map *map){

}*/

type Pair struct {
	val1 int
	val2 int
}

func main() {
	hash_workers := flag.Int("hash-workers", 1, "number of threads to do hash computation")
	data_workers := flag.Int("data-workers", 0, "number of threads to update the map")
	comp_workers := flag.Int("comp-workers", 0, "number of threads to do comparison")
	input := flag.String("input", "", "path to an input file")

	flag.Parse()

	if *input == "" {
		fmt.Println("Please specify path to an input file")
		return
	}
	if *hash_workers < 1 {
		fmt.Println("Please enter a valid number of hash-workers, hash-worker must>=1")
		return
	}

	var nums [][]int //array to store input

	//fmt.Println("hash-workers", *hash_workers)
	//fmt.Println("data-workers", *data_workers)
	//fmt.Println("comp-workers", *comp_workers)
	//fmt.Println("input", *input)

	read_file(*input, &nums)

	var trees []*Tree //array of all trees
	//construct trees
	for _, num := range nums {
		t := new_tree(num)
		trees = append(trees, t)
	}

	/*
		ch := Walker(trees[9])
		for{
			v1, ok1 := <-ch

			if !ok1 {
				fmt.Print("\nend")
				break
			}
			fmt.Print(v1," ")
		}*/
	hash_map := make(map[int][]int)
	var hash_pairs []*Pair

	start := time.Now()
	if *hash_workers == 1 {
		for id, tree := range trees {
			hash_value := hash_work(tree)
			p := Pair{hash_value, id}
			hash_pairs = append(hash_pairs, &p)
			//hash_map[hash_value] = append(hash_map[hash_value], id)
		}
		/*
			for _,p := range hash_pairs{
				fmt.Println(*p)
			}*/

	}

	//fmt.Println("len ", len(hash_map[420]))
	hash_time := time.Since(start)

	fmt.Printf("hashTime: %f\n", hash_time.Seconds())
	if *data_workers == 0 {
		os.Exit(0)
	}

	if *data_workers == 1 {
		for _, p := range hash_pairs {
			hash_map[p.val1] = append(hash_map[p.val1], p.val2)
		}
		/*
			for k, v := range hash_map{
				fmt.Printf("key: %d ->", k)
				for _, val :=range v{
					fmt.Print(val," ")
				}
				fmt.Print("\n")
			}*/
	}

	group_time := time.Since(start)
	fmt.Printf("hashGroupTime: %f\n", group_time.Seconds())

	for k, v := range hash_map {
		if len(v) > 1 {
			fmt.Printf("%d: ", k)
			for _, val := range v {
				fmt.Print(val, " ")
			}
			fmt.Print("\n")
		}

	}
	if *comp_workers == 0 {
		os.Exit(0)
	}

	n_val := len(nums) //number of trees
	var comp_matrix [][]bool
	for i := 0; i < n_val; i++ {
		comp_matrix = append(comp_matrix, make([]bool, n_val))
	}

	start_comp := time.Now()
	if *comp_workers == 1 {
		for _, v := range hash_map {

			if len(v) == 1 {
				comp_matrix[v[0]][v[0]] = true
			} else {
				//fmt.Println("map ", v)
				for i := 0; i < len(v); i++ {
					comp_matrix[v[i]][v[i]] = true
					for j := i + 1; j < len(v); j++ {
						t1 := v[i]
						t2 := v[j]
						fmt.Printf("Tree index %d %d\n", t1, t2)
						//result := CompareYaojie(trees[t1], trees[t2], 0)
						result := Compare(trees[t1], trees[t2])
						fmt.Printf("Tree index %d %d retult %d\n", t1, t2, result)
						comp_matrix[t1][t2] = result
						comp_matrix[t2][t1] = result
						//fmt.Println("here ",t1 , t2)
					}
				}
			}
		}
	}
	//time.Sleep(1600 * time.Millisecond)
	comp_time := time.Since(start_comp)
	fmt.Printf("compareTreeTime: %f\n", comp_time.Seconds())
	//fmt.Println(Compare(trees[4], trees[9]))
	//for _, i:= range(comp_matrix){
	//	fmt.Println(i)
	//}
	//fmt.Println(comp_matrix)
	group_count := 0
	for i := 0; i < n_val; i++ {
		printed := false
		if !comp_matrix[i][i] {
			continue
		}
		for j := i + 1; j < n_val; j++ {
			if comp_matrix[i][j] {
				if !printed {
					fmt.Printf("group %d: %d ", group_count, i)
					group_count++
					printed = true

				}
				comp_matrix[j][j] = false
				fmt.Print(j, " ")
			}

		}
		if printed {
			fmt.Print("\n")
		}

	}

}
