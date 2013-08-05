package neo4j

import (
	"log"
	"os"
	"os/exec"
	"testing"
)

func TestIntegration(t *testing.T) {
	root, err := Open("")
	if err != nil {
		t.Fatal("failed to open default database", err)
	}

	// Get relationship types
	rtypes, err := root.RelationshipTypes()
	if err != nil {
		t.Fatal("failed to get relationship types:", err)
	}
	if len(rtypes) != 0 {
		t.Fatalf("expected 0 relationship types, got %d", len(rtypes))
	}

	// Get the reference node
	ref, err := root.GetReferenceNode()
	if err != nil {
		t.Fatal("failed to get reference node:", err)
	}
	if ref == nil {
		t.Fatal("reference node is nil")
	}
	if id := ref.Id(); id != 0 {
		t.Fatalf("bad node id: got %d, want 0", id)
	}

	// Get node 0
	n0, err := root.GetNode(0)
	if err != nil {
		t.Fatal("failed to get 0 node:", err)
	}
	if n0 == nil {
		t.Fatal("node 0 is nil")
	}
	if id := n0.Id(); id != 0 {
		t.Fatalf("bad node id: got %d, want 0", id)
	}

	// Get a non-existent node
	_, err = root.GetNode(1)
	if err == nil {
		t.Fatal("no error fetching invalid node")
	}
	_ = err.(NodeNotFound)

	// Create a node
	newNode, err := root.CreateNode(nil)
	if err != nil {
		t.Fatal("failed to create node:", err)
	}
	newNodeId := newNode.Id()
	newNodeCopy, err := root.GetNode(newNodeId)
	if err != nil {
		t.Fatalf("failed to get node:", err)
	}
	if id := newNodeCopy.Id(); id != newNodeId {
		t.Fatalf("node ID's do not match. Got %d, want %d", id, newNodeId)
	}
}

func init() {
	stopDB := exec.Command("./neo4j/bin/neo4j", "stop")
	stopDB.Stdout = os.Stdout
	stopDB.Stderr = os.Stderr
	stopDB.Run()

	err := os.RemoveAll("./neo4j/data")
	if err != nil {
		log.Fatal("failed to remove existing database")
	}
	err = os.Mkdir("./neo4j/data", 0755)
	if err != nil {
		log.Fatal("failed to create data directory")
	}

	startDB := exec.Command("./neo4j/bin/neo4j", "start")
	startDB.Stdout = os.Stdout
	startDB.Stderr = os.Stderr
	err = startDB.Run()
	if err != nil {
		log.Fatal("failed to start Neo4j")
	}

	logRequests = true
	logResponses = true
}
