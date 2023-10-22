package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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

func go_hash_mutex(wg *sync.WaitGroup, trees []*Tree, ch1 chan Pair, m *sync.Mutex, hash_map *map[int]*[]int, start_index int, end_index int) {
	defer wg.Done()
	for i := start_index; i <= end_index; i++ {
		p := Pair{hash_work(trees[i]), i}

		//add mutex
		m.Lock()
		array, exists := (*hash_map)[p.val1]
		if exists {
			*array = append(*array, p.val2)
		} else {
			array = &[]int{p.val2}
			(*hash_map)[p.val1] = array
		}
		m.Unlock()

	}

}

func go_hash_multi_channels(wg *sync.WaitGroup, trees []*Tree, chs []chan Pair, start_index int, end_index int) {
	defer wg.Done()
	number_channels := len(chs)
	for i := start_index; i <= end_index; i++ {
		p := Pair{hash_work(trees[i]), i}
		output_ch := i % number_channels
		chs[output_ch] <- p
	}
}

func optional_dataworker(wg *sync.WaitGroup, ch1 chan Pair, hash_map *map[int]*[]int, read_write *LockFreeReadWrite, fine_grain_lock *FineGrainHashLock) {
	defer wg.Done()
	for {
		p, ok := <-ch1
		if !ok {
			return
		}
		fine_grain_lock.lock_hash(p.val1)
		read_write.gain_read()
		array, exists := (*hash_map)[p.val1]
		if exists {
			//This is not "writing" to map, just change existing key's value
			//Just need to make sure the same key's value changed sequentially using fine grained locks
			//*array.append(p.val2)
			*array = append(*array, p.val2)
			read_write.release_read()

		} else {
			read_write.release_read()
			//This is considered writing to the map, since it creates
			read_write.gain_write()
			array = &[]int{p.val2}
			(*hash_map)[p.val1] = array
			read_write.release_write()
		}
		fine_grain_lock.unlock_hash(p.val1)

	}
}

func central_manager(hash_workers int, data_workers int, trees []*Tree, hash_map *map[int]*[]int, done chan bool) {
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
		fmt.Print("Doing 1 dataworker\n")
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
			array, exists := (*hash_map)[p.val1]
			if exists {
				*array = append(*array, p.val2)
			} else {
				array = &[]int{p.val2}
				(*hash_map)[p.val1] = array
			}
		}

	} else if data_workers == hash_workers {
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
			fmt.Print("Doing n dataworker\n")
		}()
	} else {
		var fine_grain_lock FineGrainHashLock
		fine_grain_lock.init(data_workers * 3)
		data_channels := make([]chan Pair, data_workers)
		for i := 0; i < data_workers; i++ {
			data_channels[i] = make(chan Pair, hash_workers*2)
		}
		var lock_free_rw LockFreeReadWrite
		lock_free_rw.init(int32(data_workers))
		dwg := new(sync.WaitGroup)
		for i := 0; i < hash_workers; i++ {
			if re > 0 {
				end_index = start_index + work_load
				re -= 1
			} else {
				end_index = start_index + work_load - 1
			}
			wg.Add(1)
			go go_hash_multi_channels(wg, trees, data_channels, start_index, end_index)
			start_index = end_index + 1
		}
		for i := 0; i < data_workers; i++ {
			go optional_dataworker(dwg, data_channels[i], hash_map, &lock_free_rw, &fine_grain_lock)
			dwg.Add(1)
		}
		go func() {
			wg.Wait()
			for i := 0; i < data_workers; i++ {
				close(data_channels[i])
			}
			dwg.Wait()
			done <- true
			fmt.Print("Doing optional dataworker\n")
		}()

	}

}

type Pair struct {
	val1 int
	val2 int
}

type CircularBuffer struct {
	work        []Pair
	CAPACITY    int
	count       int
	read_index  int
	write_index int
}

func (buffer *CircularBuffer) init(max_load int) {
	buffer.CAPACITY = max_load
	buffer.work = make([]Pair, max_load)
	buffer.count = 0
	buffer.read_index = 0
	buffer.write_index = 0
}

func (buffer *CircularBuffer) isFull() bool {
	return buffer.count == buffer.CAPACITY
}

func (buffer *CircularBuffer) isEmpty() bool {
	return buffer.count == 0
}

// Add work and return success, full
func (buffer *CircularBuffer) add_work(work Pair) (bool, bool) {
	is_full := (buffer.count == buffer.CAPACITY)
	if is_full {
		return false, true
	} else {
		buffer.work[buffer.write_index] = work
		buffer.write_index = (buffer.write_index + 1) % buffer.CAPACITY
		buffer.count++
		return true, buffer.count == buffer.CAPACITY
	}
}

// Remove work and return work, valid (invalid means no work returned), empty
func (buffer *CircularBuffer) get_work() (Pair, bool, bool) {
	is_empty := (buffer.count == 0)
	if is_empty {
		return Pair{}, false, true
	} else {
		result := buffer.work[buffer.read_index]
		buffer.read_index = (buffer.read_index + 1) % buffer.CAPACITY
		buffer.count--
		return result, true, (buffer.count == 0)
	}
}

type FineGrainHashLock struct {
	grains int
	locks  []sync.Mutex
}

func (hash_lock *FineGrainHashLock) init(grains int) {
	hash_lock.locks = make([]sync.Mutex, grains)
	hash_lock.grains = grains
}

func (hash_lock *FineGrainHashLock) lock_hash(hash int) {
	bounded_index := hash % hash_lock.grains
	hash_lock.locks[bounded_index].Lock()
}

func (hash_lock *FineGrainHashLock) unlock_hash(hash int) {
	bounded_index := hash % hash_lock.grains
	hash_lock.locks[bounded_index].Unlock()
}

type LockFreeReadWrite struct {
	//Use basic version for now
	lock           sync.Mutex
	max_readers    int32
	resource_count atomic.Int32
	rwLock         sync.RWMutex
}

func (readWrite *LockFreeReadWrite) init(readers int32) {
	readWrite.max_readers = readers
	readWrite.resource_count.Store(int32(readers))
}

func (readWrite *LockFreeReadWrite) gain_read() {
	//readWrite.lock.Lock()
	for {
		after_minus := readWrite.resource_count.Add(-1)
		if after_minus >= 0 {
			return
		} else {
			//Give up and return things, wait for others to finish then retry
			readWrite.resource_count.Add(1)
			for {
				after_return := readWrite.resource_count.Load()
				if after_return > 0 {
					break
				}
			}
		}
	}
	//readWrite.rwLock.RLock()
}

func (readWrite *LockFreeReadWrite) release_read() {
	//readWrite.lock.Unlock()
	readWrite.resource_count.Add(1)
	// readWrite.rwLock.RUnlock()
}

func (readWrite *LockFreeReadWrite) gain_write() {
	//readWrite.lock.Lock()
	for {
		after_minus := readWrite.resource_count.Add(-readWrite.max_readers)
		if after_minus == 0 {
			//gain access successful, exit
			return
		} else if after_minus < 0 && after_minus > -readWrite.max_readers {
			//First writer, though not gained, wait for anyone else to exit
			for {
				wait_for_return := readWrite.resource_count.Load()
				if wait_for_return == 0 {
					//gain access successful, exit
					return
				}
			}
		} else {
			//Give up and return things, wait for others to finish then retry
			readWrite.resource_count.Add(readWrite.max_readers)
			for {
				after_return := readWrite.resource_count.Load()
				if after_return > 0 {
					break
				}
			}
		}
	}
	// readWrite.rwLock.Lock()
}

func (readWrite *LockFreeReadWrite) release_write() {
	//readWrite.lock.Unlock()
	readWrite.resource_count.Add(readWrite.max_readers)
	// readWrite.rwLock.Unlock()
}

type ReliableWake struct {
	wake_cond      sync.Cond
	someone_awaken bool
}

func (wakeup *ReliableWake) init() {
	wakeup.wake_cond = *sync.NewCond(&sync.Mutex{})
	wakeup.someone_awaken = false
}

func (wakeup *ReliableWake) Signal() {
	for {
		wakeup.wake_cond.Signal()
		wakeup.wake_cond.L.Lock()
		if wakeup.someone_awaken {
			wakeup.someone_awaken = false
			wakeup.wake_cond.L.Unlock()
			break
		}
		wakeup.wake_cond.L.Unlock()
	}
}

func (wakeup *ReliableWake) Broadcast() {
	for {
		wakeup.wake_cond.Broadcast()
		wakeup.wake_cond.L.Lock()
		if wakeup.someone_awaken {
			wakeup.someone_awaken = false
			wakeup.wake_cond.L.Unlock()
			break
		}
		wakeup.wake_cond.L.Unlock()
	}
}

func (wakeup *ReliableWake) Wait() {
	wakeup.wake_cond.L.Lock()
	wakeup.wake_cond.Wait()
	wakeup.someone_awaken = true
	wakeup.wake_cond.L.Unlock()
}

type ConcurrentBuffer struct {
	lock         sync.Mutex
	Capacity     int
	refill_limit int
	work_sleep   int
	main_sleep   bool
	last         bool
	work         CircularBuffer
	main_ch      chan bool
	work_ch      chan bool
	wake_main_r  ReliableWake
	wake_work_r  ReliableWake
}

func (buffer *ConcurrentBuffer) init(capacity, refill, workers int) {
	buffer.Capacity = capacity
	buffer.work.init(capacity)
	buffer.refill_limit = refill
	buffer.work_sleep = 0
	buffer.main_sleep = false
	buffer.last = false
	buffer.main_ch = make(chan bool)
	buffer.work_ch = make(chan bool, workers)
	buffer.wake_main_r.init()
	buffer.wake_work_r.init()
}

// Insert work and return number of work inserted, and if buffer emptied
// Main thread sleeps after inserting work
func (concurrent_buffer *ConcurrentBuffer) insert_work(buffer *CircularBuffer, isLast bool) (int, bool) {
	inserted := 0
	concurrent_buffer.lock.Lock()
	need_awake := concurrent_buffer.work_sleep
	concurrent_buffer.work_sleep = 0

	for {
		if buffer.isEmpty() {
			concurrent_buffer.last = isLast
			concurrent_buffer.lock.Unlock()
			// if need_awake  {
			// 	//fmt.Print("Empty wake worker\n")
			// 	concurrent_buffer.wake_work_r.Broadcast()
			// }
			for {
				if need_awake == 0 {
					break
				} else {
					concurrent_buffer.work_ch <- true
					need_awake--
				}
			}
			return inserted, true
		} else {
			if concurrent_buffer.work.isFull() {
				if inserted == 0 && !isLast {
					concurrent_buffer.main_sleep = true
				}
				concurrent_buffer.lock.Unlock()
				// if need_awake {
				// 	//fmt.Printf("Full wake worker\n")
				// 	concurrent_buffer.wake_work_r.Broadcast()
				// }
				for {
					if need_awake == 0 {
						break
					} else {
						concurrent_buffer.work_ch <- true
						need_awake--
					}
				}
				return inserted, false
			}
			to_be_added, _, _ := buffer.get_work()
			concurrent_buffer.work.add_work(to_be_added)
			inserted++
		}
	}
}

// Return work and valid (invalid if no work in buffer) and final done
func (concurrent_buffer *ConcurrentBuffer) acquire_work() (Pair, bool, bool) {
	concurrent_buffer.lock.Lock()
	if concurrent_buffer.work.isEmpty() {
		is_last := concurrent_buffer.last
		if !is_last {
			concurrent_buffer.work_sleep++
		}
		concurrent_buffer.lock.Unlock()
		return Pair{}, false, is_last
	} else {
		result, _, is_empty := concurrent_buffer.work.get_work()
		final_done := is_empty && concurrent_buffer.last
		need_to_wake_main := concurrent_buffer.main_sleep && concurrent_buffer.work.count <= concurrent_buffer.refill_limit
		if need_to_wake_main {
			concurrent_buffer.main_sleep = false
		}
		concurrent_buffer.lock.Unlock()
		if need_to_wake_main {
			concurrent_buffer.main_ch <- true
		}
		return result, true, final_done
	}
}

// spawn goroutine for each pair of trees comparison
func go_comp(wg *sync.WaitGroup, pair Pair, trees []*Tree, hash_log [][]int, result_map map[int]([][]bool)) {
	defer wg.Done()
	t1 := pair.val1
	t2 := pair.val2
	result := Compare_byStack(trees[t1], trees[t2])
	key := hash_log[t1][0]
	id1 := hash_log[t1][1]
	id2 := hash_log[t2][1]
	(result_map[key])[id1][id2] = result
	(result_map[key])[id2][id1] = result
}

func comp_work(wg *sync.WaitGroup, trees []*Tree, hash_log [][]int, result_map map[int]([][]bool), work_buffer *ConcurrentBuffer) {
	defer wg.Done()
	sleep := 0

	for {
		pair, valid, final := work_buffer.acquire_work()
		if !valid {
			if final {
				return
			}
			<-work_buffer.work_ch
			sleep++
			continue
		}
		t1 := pair.val1
		t2 := pair.val2
		result := Compare_byStack(trees[t1], trees[t2])
		key := hash_log[t1][0]
		id1 := hash_log[t1][1]
		id2 := hash_log[t2][1]
		(result_map[key])[id1][id2] = result
		(result_map[key])[id2][id1] = result
	}
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

	hash_map := make(map[int]*[]int)
	var hash_pairs []*Pair

	start := time.Now()
	if *hash_workers == 1 {
		for id, tree := range trees {
			hash_value := hash_work(tree)
			p := Pair{hash_value, id}
			hash_pairs = append(hash_pairs, &p)

		}

	} else {
		//fmt.Println("parellel hash")
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
			array_append := append(*hash_map[p.val1], p.val2)
			hash_map[p.val1] = &array_append
		}
	}

	group_time := time.Since(start)
	fmt.Printf("hashGroupTime: %f\n", group_time.Seconds())

	for k, v := range hash_map {
		if len(*v) > 1 {
			fmt.Printf("%d: ", k)
			for _, val := range *v {
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
	hash_log := make([][]int, n_val)       // each line: hash key&id as in hash_map[key][id]
	result_map := make(map[int]([][]bool)) //small matrix for each key in hash_map
	for k, v := range hash_map {
		if len(*v) == 1 {
			continue
		}
		var a [][]bool
		for i, id := range *v {
			var b []bool
			for j := 0; j < len(*v); j++ {
				if i == j {
					b = append(b, true)
				} else {
					b = append(b, false)
				}

			}
			a = append(a, b)
			hash_log[id] = append(hash_log[id], k, i)
		}
		result_map[k] = a
	}

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

		for k, v := range hash_map {
			l := len(*v)
			if l > 1 {
				for i := 0; i < l; i++ {
					t1 := (*v)[i]
					id1 := hash_log[t1][1]
					//(result_map[k])[id1][id1] = true
					for j := i + 1; j < l; j++ {
						t2 := (*v)[j]
						id2 := hash_log[t2][1]
						result := Compare_byStack(trees[t1], trees[t2])
						(result_map[k])[id1][id2] = result
						(result_map[k])[id2][id1] = result

					}
				}
			}
		}
	} else {
		wg := new(sync.WaitGroup)
		//spawn one goroutine for each pair of trees
		/*for _, v := range hash_map {
			l := len(v)
			if l > 1 {
				for i := 0; i < l; i++ {
					for j := i + 1; j < l; j++ {
						wg.Add(1)
						pair := Pair{v[i], v[j]}
						go go_comp(wg, pair, trees, hash_log, result_map)
					}
				}
			}
		}*/
		//use work buffer
		sleep := 0
		local_buffer := CircularBuffer{}
		local_buffer.init(*comp_workers)
		shared_buffer := ConcurrentBuffer{}
		shared_buffer.init(*comp_workers, *comp_workers-1, *comp_workers)
		total := 0
		//go_sleep := false
		inserted := 0
		//emptied := false

		for i := 0; i < *comp_workers; i++ {
			wg.Add(1)
			go comp_work(wg, trees, hash_log, result_map, &shared_buffer)
		}
		//total := 0
		for _, v := range hash_map {
			l := len(*v)
			if l > 1 {
				for i := 0; i < l; i++ {
					for j := i + 1; j < l; j++ {
						pair := Pair{(*v)[i], (*v)[j]}
						_, full := local_buffer.add_work(pair)
						if full {
							for {
								inserted, _ = shared_buffer.insert_work(&local_buffer, false)
								total += inserted
								if inserted > 0 {
									//fmt.Printf("Inserted %d total %d\n", inserted, total)
									break
								} else {
									<-shared_buffer.main_ch
									sleep++
									//fmt.Printf("Main sleep %d\n", sleep)
								}
							}
						}
					}
				}
			}
		}

		for {
			_, empty := shared_buffer.insert_work(&local_buffer, true)
			if empty {
				break
			}
		}

		//shared_buffer.wake_work_r.Broadcast()
		//fmt.Println("sleep", sleep)
		//fmt.Println("total", total)
		wg.Wait()
		//close(ch_m)
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

	for k, v := range result_map {
		l := len(v)
		for i := 0; i < l; i++ {
			printed := false
			if !v[i][i] {
				continue
			}
			for j := i + 1; j < l; j++ {
				if v[i][j] {
					if !printed {
						fmt.Printf("group %d: %d ", group_count, (*hash_map[k])[i])
						group_count++
						printed = true
					}
					v[j][j] = false
					fmt.Print((*hash_map[k])[j], " ")
				}
			}
			if printed {
				fmt.Print("\n")
			}
		}
	}

}
