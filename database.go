package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
)


// constants for node types and sizes
const (
		HEADER_SIZE = 4
		POINTER_SIZE = 8
		OFFSET_SIZE = 2
		KEY_VALUE_HEADER = 4
		BNODE_NODE = 1 // no values
		BNODE_LEAF = 2 // has values
		BTREE_PAGE_SIZE = 4096
		BTREE_MAX_KEY_SIZE = 1000
		BTREE_MAX_VAL_SIZE = 3000
)

// BNode represents a B+ tree node as a slice of bytes.
// This allows direct manipulation and easy serialization to disk.

// BNode memory layout
// | type | nkeys |  pointers  |   offsets  | key-values | unused |
// |  2B  |   2B  | nkeys * 8B | nkeys * 2B |     ...    |        |
//
// | klen | vlen | key | val |
// |  2B  |  2B  | ... | ... |

type BNode []byte // can be dumped too disk

type BTree struct{
	root uint64
	get func(uint64) []byte // dereference a pointer
	new func([]byte) uint64 // allocate a new page
	del func(uint64)		// deallocate a page
}

func assert(condition bool, msg string){
	if !condition{
		panic(msg)
	}
}
func init(){
	node1max := HEADER_SIZE + POINTER_SIZE + OFFSET_SIZE + KEY_VALUE_HEADER + 
				BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE

    assert(node1max <= BTREE_PAGE_SIZE,		
		   "Maximum key-value size exceeds node size")

}

// returns type of Bnode:  leaf or internal node
func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node[0:2])
}
func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node[2:4])
}

func (node BNode) setHeader (btype uint16, nkeys uint16){
	binary.LittleEndian.PutUint16(node[0:2], btype)
	binary.LittleEndian.PutUint16(node[2:4], nkeys)
}

//-------- Child Pointer Functions -------------


// returns the child pointer at index idx in node
func (node BNode) getPtr(idx uint16) uint64{
	assert(idx < node.nkeys(), "Index is out of bounds in getPrt")
	pos := HEADER_SIZE + POINTER_SIZE*idx
	return binary.LittleEndian.Uint64(node[pos:])
}


// sets the child pointer at index idx in node
func (node BNode) setPtr(idx uint16, val uint64){
	assert(idx < node.nkeys(), "Index is out of bounds in setPtr")
	pos := HEADER_SIZE + POINTER_SIZE*idx
	binary.LittleEndian.PutUint64(node[pos:], val)
}

//-------- Offset Functions -------------

//  calculates the position in the node's byte slice where the offset 
//  for the node specified by idx
func getOffsetPos(node BNode, idx uint16) uint16 {
    assert(1 <= idx && idx <= node.nkeys(), 
			"Index out of bounds in offsetPosition")
	offsetsStart := HEADER_SIZE + POINTER_SIZE*node.nkeys()
	return  offsetsStart + OFFSET_SIZE*(idx-1)
}

// retrieves the value of the offest for node specified by idx
func (node BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}

	return binary.LittleEndian.Uint16(node[getOffsetPos(node, idx):])
}


// sets the value of the offest for node specified by idx
func (node BNode) setOffset(idx uint16, offset uint16){
	offsetStart := getOffsetPos(node, idx)
	binary.LittleEndian.PutUint16(node[offsetStart:], offset)
}

//-------- Key-Value functions ------------------

// calculates the position in the node's byte slice where the key-value data 
// for  the specified index starts.
func (node BNode) kvPosition(idx uint16) uint16{
   //  assert(1 <= idx && idx <= node.nkeys(), 
			// "Index out of bounds in kvPosition")

	return (HEADER_SIZE + POINTER_SIZE*node.nkeys() + 
		   OFFSET_SIZE*node.nkeys() + node.getOffset(idx))
}

// retrieves the key  at the specified index in the node.
func (node BNode) getKey(idx uint16) []byte {
    assert(idx <= node.nkeys(), 
			"Index out of bounds in getKey")
	kvPos := node.kvPosition(idx)
	keyStartPos := kvPos + HEADER_SIZE
	keyByteLen := binary.LittleEndian.Uint16(node[kvPos:])
	return node[keyStartPos:][:keyByteLen]
}

// retrieves the value at the specified index in the node.
func (node BNode) getVal(idx uint16) []byte{
    assert(idx <= node.nkeys(), 
			"Index out of bounds in getVal")
	kvPos := node.kvPosition(idx)
	keyByteLen := binary.LittleEndian.Uint16(node[kvPos:])
	valByteLen := binary.LittleEndian.Uint16(node[kvPos+2:])

	valStartPos := kvPos + HEADER_SIZE + keyByteLen

	return node[valStartPos:][:valByteLen]
}

// reuturns the total amount of bytes used in the node
func (node BNode) nbytes() uint16 {
	return node.kvPosition(node.nkeys())
}

//-------- Node Lookup Functions ------------------


// nodeLookupLE performs a linear search within the node to find the index of 
// the key that is less than or equal to the given key. This is used to 
// determine the child pointer to follow during searches.
func seekNodeLE(node BNode, key []byte) uint16  {
	nkeys := node.nkeys()
	result := uint16(0) // first key is copy of parent node
	for i := range nkeys{
		diff := bytes.Compare(node.getKey(i), key)
		if diff <= 0 {
			result = i
		}
		if diff >= 0 {
			break
		}
	}
	return result
}

//-------- Node Update Functions ------------------

// nodeAppendKV appends a single key-value pair and its pointer to the new 
// node at the specified index.
func nodeAppendkv(
	newNode BNode, idx uint16, childPtr uint64, key []byte, val []byte){

	newNode.setPtr(idx, childPtr) // replace parent pointer

	kvPos := newNode.kvPosition(idx)

	binary.LittleEndian.PutUint16(newNode[kvPos:], uint16(len(key)))
	binary.LittleEndian.PutUint16(newNode[kvPos+2:], uint16(len(val)))

	copy(newNode[kvPos+KEY_VALUE_HEADER:], key)
	copy(newNode[kvPos+KEY_VALUE_HEADER+uint16(len(key)):], val)
	
	offsetBeforeInsert := newNode.getOffset(idx)
	newNode.setOffset(idx+1, offsetBeforeInsert + KEY_VALUE_HEADER +
					  uint16((len(key)+len(val))))

}

// copies a range of key-value pairs and their pointers from the old node to 
// the new node. It copies 'count' number of entries starting from index 
//'srcIdx' in the old node, to index 'dstIdx' in the new node.
func nodeAppendRange(
	newNode BNode, oldNode BNode, 
	destIdx  uint16, srcIndex uint16, count uint16,
	){

	for i := range count{
		oldIdx := srcIndex + i
		newIdx := destIdx + i

		childPtr := oldNode.getPtr(oldIdx)
		newNode.setPtr(newIdx, childPtr)

		offsetBeforeInsert := newNode.getOffset(newIdx)

		oldKVPos := oldNode.kvPosition(oldIdx)
		newKVPos := newNode.kvPosition(newIdx)

		keyLen := binary.LittleEndian.Uint16(oldNode[oldKVPos:])
		valLen := binary.LittleEndian.Uint16(oldNode[oldKVPos+2:])

		kvTotalSize := KEY_VALUE_HEADER + keyLen + valLen
		copy(newNode[newKVPos:], oldNode[oldKVPos:oldKVPos+kvTotalSize])

		newNode.setOffset(newIdx+1, offsetBeforeInsert+kvTotalSize)



	}
}
// add a new key to a leaf node using copy on write
func insertLeaf(newNode BNode, oldNode BNode, insertIdx uint16,
				key []byte, val []byte){
	newNode.setHeader(BNODE_LEAF, oldNode.nkeys()+1)

	nodeAppendRange(newNode, oldNode, 0, 0, insertIdx) // copy old 0- n-1 to new

	nodeAppendkv(newNode, insertIdx, 0, key, val)

	nodeAppendRange(newNode, oldNode, insertIdx+ 1, insertIdx ,
								oldNode.nkeys()-insertIdx) // copy the rest

}

// ------------------------- Example Usage -------------------------

func main() {
    // Example usage of the above functions.
    // Create an old node and a new node.
    oldNode := make(BNode, BTREE_PAGE_SIZE)
    newNode := make(BNode, BTREE_PAGE_SIZE)

    // Initialize the old node as a leaf node with one key-value pair.
    oldNode.setHeader(BNODE_LEAF, 1)
    oldNode.setPtr(0, 0)
    oldNode.setOffset(1, KEY_VALUE_HEADER+uint16(len("key1")+len("value1")))
    kvPos := oldNode.kvPosition(0)
    binary.LittleEndian.PutUint16(oldNode[kvPos:], uint16(len("key1")))
    binary.LittleEndian.PutUint16(oldNode[kvPos+2:], uint16(len("value1")))
    copy(oldNode[kvPos+4:], []byte("key1"))
    copy(oldNode[kvPos+4+uint16(len("key1")):], []byte("value1"))

    // Insert a new key-value pair into the new node at index 1.
    insertLeaf(newNode, oldNode, 1, []byte("key2"), []byte("value2"))

    // Print the keys and values in the new node.
    for i := uint16(0); i < newNode.nkeys(); i++ {
        key := newNode.getKey(i)
        val := newNode.getVal(i)
        fmt.Printf("Key: %s, Value: %s\n", key, val)
    }
}
