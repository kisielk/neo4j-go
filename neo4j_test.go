package neo4j

import (
	"testing"
)

func TestIntegration(t *testing.T) {
	root, err := Open("")
	if err != nil {
		t.Fatal("failed to open default database", err)
	}

	rtypes, err := root.RelationshipTypes()
	if err != nil {
		t.Fatal("failed to get relationship types:", err)
	}

	if len(rtypes) != 0 {
		t.Fatalf("expected 0 relationship types, got %d", len(rtypes))
	}

	n0, err := root.GetNode(0)
	if err != nil {
		t.Fatal("failed to get 0 node:", err)
	}
	if n0 == nil {
		t.Fatal("node 0 is nil")
	}

	_, err = root.GetNode(1)
	if err == nil {
		t.Fatal("no error fetching invalid node")
	}
	_ = err.(NodeNotFound)
}
