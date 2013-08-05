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

	_, err = root.GetNode(1)
	if err == nil {
		t.Fatal("no error fetching invalid node")
	}
	_ = err.(NodeNotFound)
}
