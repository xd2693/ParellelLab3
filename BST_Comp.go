package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// A Tree is a binary tree with integer values.
type Tree struct {
	Left  *Tree
	Value int
	Right *Tree
}

// Returns
type InOrderTraverser struct {
	Root        *Tree `default:"nil"`
	RemainStack []*Tree
	InitDone    bool `default:"false"`
}

func initTraverser(t *Tree, tv *InOrderTraverser) {
	tv.Root = t
	node := t
	for ; node != nil; node = node.Left {
		tv.RemainStack = append(tv.RemainStack, node)
	}
	tv.InitDone = true
	return
}

func traverseOnestep(tv *InOrderTraverser) *Tree {
	if !tv.InitDone {
		fmt.Print("Init this traverser\n")
	}
	var result *Tree = nil
	var length int = len(tv.RemainStack)
	if length > 0 {
		result = tv.RemainStack[length-1]
		tv.RemainStack = tv.RemainStack[:length-1]
		for node := result.Right; node != nil; node = node.Left {
			tv.RemainStack = append(tv.RemainStack, node)
		}
	} else {
		tv.InitDone = false
	}
	// if result != nil {
	// fmt.Printf("Tree node is %d\n", result.Value)
	// }
	return result
}

// Walk traverses a tree depth-first,
// sending each Value on a channel.
/*
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
}*/

func Walk(t *Tree, tree *[]int) {
	if t == nil {
		return
	}
	Walk(t.Left, tree)
	*tree = append(*tree, t.Value)
	Walk(t.Right, tree)
}

func Walker(t *Tree) []int {
	var tree []int
	Walk(t, &tree)
	return tree
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
/*
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
}*/
/*
func Compare(t1, t2 *Tree) bool {
	c1, c2 := Walker(t1), Walker(t2)
	if len(c1) != len(c2) {
		return false
	}
	for i := 0; i < len(c1); i++ {
		if c1[i] != c2[i] {
			return false
		}
	}
	return true
}*/

func Compare_byStack(t1, t2 *Tree) bool {
	var tv1, tv2 InOrderTraverser
	initTraverser(t1, &tv1)
	initTraverser(t2, &tv2)
	var node1, node2 *Tree
	for {
		node1 = traverseOnestep(&tv1)
		node2 = traverseOnestep(&tv2)
		if (node1 != nil && node2 == nil) || (node1 == nil && node2 != nil) {
			return false
		} else if node1 == nil && node2 == nil {
			return true
		} else if node1.Value != node2.Value {
			return false
		}
	}
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
	for _, value := range ch {
		new_value = value + 2
		hash = (hash*new_value + new_value) % 1000
	}
	return hash
}

func go_hash_only(wg *sync.WaitGroup, trees []*Tree, start_index int, end_index int) {
	defer wg.Done()
	for i := start_index; i <= end_index; i++ {
		hash_work(trees[i])

	}
}

func go_hash_channel(wg *sync.WaitGroup, trees []*Tree, ch1 chan Pair, start_index int, end_index int) {
	defer wg.Done()
	for i := start_index; i <= end_index; i++ {
		p := Pair{hash_work(trees[i]), i}
		ch1 <- p

	}
}

func go_hash_mutex(wg *sync.WaitGroup, trees []*Tree, ch1 chan Pair, m *sync.Mutex, hash_map *map[int][]int, start_index int, end_index int) {
	defer wg.Done()
	for i := start_index; i <= end_index; i++ {
		p := Pair{hash_work(trees[i]), i}

		//add mutex
		m.Lock()
		(*hash_map)[p.val1] = append((*hash_map)[p.val1], p.val2)
		m.Unlock()

	}

}

func central_manager(hash_workers int, data_workers int, trees []*Tree, hash_map *map[int][]int, done chan bool) {
	ch1 := make(chan Pair, hash_workers)
	//ch2 := make(chan int, 16)
	wg := new(sync.WaitGroup)

	// add mutex
	var mu sync.Mutex
	n_vals := len(trees)
	re := n_vals % hash_workers
	work_load := n_vals / hash_workers
	start_index := 0
	end_index := 0

	if data_workers == 0 {
		for i := 0; i < hash_workers; i++ {
			if re > 0 {
				end_index = start_index + work_load
				re -= 1
			} else {
				end_index = start_index + work_load - 1
			}

			wg.Add(1)
			go go_hash_only(wg, trees, start_index, end_index)
			start_index = end_index + 1
		}
		go func() {
			wg.Wait()
			done <- true //add for mutex
			close(ch1)
		}()

	} else if data_workers == 1 {
		for i := 0; i < hash_workers; i++ {
			if re > 0 {
				end_index = start_index + work_load
				re -= 1
			} else {
				end_index = start_index + work_load - 1
			}

			wg.Add(1)
			go go_hash_channel(wg, trees, ch1, start_index, end_index)
			start_index = end_index + 1
		}
		go func() {
			wg.Wait()
			close(ch1)
		}()
		for {
			p, ok := <-ch1
			if !ok {
				done <- true
				return
			}
			(*hash_map)[p.val1] = append((*hash_map)[p.val1], p.val2)
		}

	} else {
		for i := 0; i < hash_workers; i++ {
			if re > 0 {
				end_index = start_index + work_load
				re -= 1
			} else {
				end_index = start_index + work_load - 1
			}

			wg.Add(1)
			//go go_hash(wg, trees, ch2, ch1)
			//add for mutex
			go go_hash_mutex(wg, trees, ch1, &mu, hash_map, start_index, end_index)
			start_index = end_index + 1
		}
		go func() {
			wg.Wait()
			done <- true //add for mutex
			close(ch1)
		}()
	}

}

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

	read_file(*input, &nums)

	var trees []*Tree //array of all trees
	//construct trees
	for _, num := range nums {
		t := new_tree(num)
		trees = append(trees, t)
	}

	hash_map := make(map[int][]int)
	var hash_pairs []*Pair

	start := time.Now()
	if *hash_workers == 1 {
		for id, tree := range trees {
			hash_value := hash_work(tree)
			p := Pair{hash_value, id}
			hash_pairs = append(hash_pairs, &p)

		}

	} else {
		fmt.Println("parellel hash")
		done := make(chan bool)
		go central_manager(*hash_workers, *data_workers, trees, &hash_map, done)
		<-done
	}

	//fmt.Println("len ", len(hash_map[420]))
	hash_time := time.Since(start)

	fmt.Printf("hashTime: %f\n", hash_time.Seconds())
	if *data_workers == 0 {
		os.Exit(0)
	}

	if *hash_workers == 1 && *data_workers == 1 {
		for _, p := range hash_pairs {
			hash_map[p.val1] = append(hash_map[p.val1], p.val2)
		}
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
	/*var comp_matrix [][]bool
	for i := 0; i < n_val; i++ {
		comp_matrix = append(comp_matrix, make([]bool, n_val))
	}*/

	/*result_map := make(map[int]([][]bool))
	for k, v := range hash_map {
		if len(v) == 1 {
			continue
		}
		var a [][]bool
		for i := 0; i < len(v); i++{
			var b []bool
			for j := 0; j < len(v); j++{
				b = append(b, false)
			}
			a = append(a, b)
		}
		result_map[k] = append(result_map[k], a)
	}*/
	results := make([][]int, n_val)
	start_comp := time.Now()
	if *comp_workers == 1 {
		/*for _, v := range hash_map {

			if len(v) == 1 {
				comp_matrix[v[0]][v[0]] = true
			} else {
				for i := 0; i < len(v); i++ {
					comp_matrix[v[i]][v[i]] = true
					for j := i + 1; j < len(v); j++ {
						t1 := v[i]
						t2 := v[j]
						result := Compare_byStack(trees[t1], trees[t2])
						comp_matrix[t1][t2] = result
						comp_matrix[t2][t1] = result

					}
				}
			}
		}*/
		for _, v := range hash_map {
			if len(v) > 1 {
				for i := 0; i < len(v); i++ {
					for j := i + 1; j < len(v); j++ {
						t1 := v[i]
						t2 := v[j]
						result := Compare_byStack(trees[t1], trees[t2])
						if result {
							results[t1] = append(results[t1], t2)
							results[t2] = append(results[t2], t1)
						}

					}
				}
			}
		}
	}

	comp_time := time.Since(start_comp)
	fmt.Printf("compareTreeTime: %f\n", comp_time.Seconds())

	group_count := 0
	/*for i := 0; i < n_val; i++ {
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

	}*/
	for i := 0; i < n_val; i++ {
		if len(results[i]) == 0 {
			continue
		}
		fmt.Printf("group %d: %d ", group_count, i)
		group_count++
		for _, j := range results[i] {
			fmt.Print(j, " ")
			results[j] = nil
		}
		fmt.Print("\n")
	}

}
