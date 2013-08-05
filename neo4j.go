package neo4j

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
)

// logging flags, for internal use
var (
	logRequests  bool
	logResponses bool
)

const DefaultAddress = "http://localhost:7474/db/data/"

type M map[string]interface{}

type neo4j struct {
	SelfURL string `json:"self,omitempty"`
	Client  *http.Client
}

func marshallBody(v interface{}) (io.Reader, error) {
	var body io.Reader = nil
	jsonValue, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	body = bytes.NewReader(jsonValue)
	return body, nil
}

func (n neo4j) request(method string, urlStr string, body interface{}, result interface{}) error {
	var jsonBody io.Reader
	if body != nil {
		var err error
		jsonBody, err = marshallBody(body)
		if err != nil {
			return err
		}
	}

	if logRequests {
		log.Println(method, urlStr)
	}

	r, err := http.NewRequest(method, urlStr, jsonBody)
	if err != nil {
		return err
	}

	r.Header.Add("Accept", "application/json")
	if method == "PUT" {
		r.Header.Add("Content-Type", "application/json")
	}

	resp, err := n.Client.Do(r)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var reader io.Reader = resp.Body
	if logResponses {
		log.Println(resp.StatusCode)
		reader = io.TeeReader(resp.Body, os.Stderr)
	}

	dec := json.NewDecoder(reader)
	if resp.StatusCode >= 400 && resp.StatusCode < 500 {
		var respError Neo4jError
		err := dec.Decode(&respError)
		if err != nil {
			return fmt.Errorf("error decoding server error: %s", err)
		}
		return specificError(respError)
	}

	err = dec.Decode(result)
	if err != nil {
		return fmt.Errorf("error decoding server response: %s", err)
	}
	return nil
}

func (n neo4j) node(method, u string, properties M) (*Node, error) {
	var node Node
	err := n.request(method, u, properties, &node)
	if err != nil {
		return nil, err
	}
	return &node, nil
}

func (n neo4j) properties(method, u string, properties M) (M, error) {
	var m M
	err := n.request(method, u, properties, &m)
	if err != nil {
		return nil, err
	}
	return m, nil
}

func (n neo4j) relationship(method, u string, properties M) (*Relationship, error) {
	var r Relationship
	err := n.request("POST", u, properties, &r)
	if err != nil {
		return nil, err
	}
	return &r, nil
}

func (n neo4j) relationships(method, u string) ([]Relationship, error) {
	var r []Relationship
	err := n.request(method, u, nil, &r)
	if err != nil {
		return nil, err
	}
	return r, nil
}

type NodeNotFound string

func (n NodeNotFound) Error() string {
	return string(n)
}

type Neo4jError struct {
	Message    string   `json:"message"`
	Exception  string   `json:"exception"`
	Fullname   string   `json:"fullname"`
	Stacktrace []string `json:"stacktrace"`
}

func (e Neo4jError) Error() string {
	return e.Exception + ": " + e.Message
}

func specificError(e Neo4jError) error {
	switch e.Exception {
	case "NodeNotFoundException":
		return NodeNotFound(e.Message)
	}
	return e
}

type ServiceRoot struct {
	serviceRoot
}

func Open(addr string) (*ServiceRoot, error) {
	if len(addr) == 0 {
		addr = DefaultAddress
	}
	var root ServiceRoot
	neo := neo4j{SelfURL: addr, Client: &http.Client{}}
	err := neo.request("GET", neo.SelfURL, nil, &root)
	if err != nil {
		return nil, err
	}
	root.neo4j = neo
	return &root, nil
}

func (r ServiceRoot) RelationshipTypes() ([]string, error) {
	var types []string
	err := r.request("GET", r.RelationshipTypesURL, nil, &types)
	if err != nil {
		return nil, err
	}
	return types, nil
}

type cypherQuery struct {
	Query  string `json:"query"`
	Params M      `json:"params"`
}

func (r ServiceRoot) Cypher(query string, params M) (M, error) {
	var m M
	err := r.request("POST", r.CypherURL, cypherQuery{query, params}, &m)
	if err != nil {
		return nil, err
	}
	return m, nil
}

func (r ServiceRoot) CreateNode(properties M) (*Node, error) {
	return r.node("POST", r.NodeURL, properties)
}

func (r ServiceRoot) GetReferenceNode() (*Node, error) {
	return r.node("GET", r.ReferenceNodeURL, nil)
}

func (r ServiceRoot) GetNode(id int) (*Node, error) {
	return r.node("GET", fmt.Sprintf("%s/%d", r.NodeURL, id), nil)
}

func (r ServiceRoot) DeleteNode(id int) error {
	return r.request("DELETE", fmt.Sprintf("%s/%d", r.NodeURL, id), nil, nil)
}

func (n ServiceRoot) GetRelationship(id uint) (*Relationship, error) {
	// Wait till https://github.com/neo4j/community/issues/750 is resolved
	/*
		r, err := n.request("GET", n.RelationshipURL, nil)
		if resp != nil {
			defer resp.Body.Close()
		}
		if err != nil {
			return nil ,err
		}
	*/
	return nil, nil
}

func (n ServiceRoot) DeleteRelationship(id uint) error {
	// See above
	return nil
}

type serviceRoot struct {
	neo4j
	NodeURL              string `json:"node"`
	ReferenceNodeURL     string `json:"reference_node,omitempty"`
	NodeIndexURL         string `json:"node_index"`
	RelationshipIndexURL string `json:"relationship_index"`
	ExtensionsInfoURL    string `json:"extensions_info"`
	RelationshipTypesURL string `json:"relationship_types"`
	BatchURL             string `json:"batch"`
	CypherURL            string `json:"cypher"`
	Neo4jVersion         string `json:"neo4j_version"`
}

type Node struct {
	node
}

// Id returns the unique node identifier.
// It returns -1 if the identifier cannot be determined.
func (n Node) Id() int {
	s := strings.Split(n.SelfURL, "/")
	id, err := strconv.ParseInt(s[len(s)-1], 10, 64)
	if err != nil {
		return -1
	}
	return int(id)
}

func (n Node) GetProperties() (M, error) {
	return n.properties("GET", n.PropertiesURL, nil)
}

func (n Node) SetProperty(p string, v interface{}) error {
	return n.request("PUT", n.PropertiesURL+"/"+p, v, nil)
}

func (n Node) SetProperties(p M) error {
	return n.request("PUT", n.PropertiesURL, p, nil)
}

func (n Node) DeleteProperties() error {
	return n.request("DELETE", n.PropertiesURL, nil, nil)
}

func (n Node) DeleteProperty(p string) error {
	return n.request("DELETE", n.PropertiesURL+"/"+p, nil, nil)
}

func (n Node) CreateRelationship(to *Node, rtype string, properties M) (*Relationship, error) {
	args := M{
		"to":   to.SelfURL,
		"type": rtype,
		"data": properties,
	}
	return n.relationship("POST", n.CreateRelationshipURL, args)
}

func (n Node) GetIncomingRelationships() ([]Relationship, error) {
	return n.relationships("GET", n.IncomingRelationshipsURL)
}

func (n Node) GetOutgoingRelationships() ([]Relationship, error) {
	return n.relationships("GET", n.OutgoingRelationshipsURL)
}

func (n Node) GetAllRelationships() ([]Relationship, error) {
	return n.relationships("GET", n.AllRelationshipsURL)
}

func (n Node) GetTypedRelationships(types []string) ([]Relationship, error) {
	return n.relationships("GET", n.AllRelationshipsURL+"/"+strings.Join(types, "%26")) // %26 = &
}

type node struct {
	neo4j
	SelfURL                       string `json:"self"`
	PagedTraverseURL              string `json:"paged_traverse"`
	OutgoingRelationshipsURL      string `json:"outgoing_relationships"`
	TraverseURL                   string `json:"traverse"`
	AllTypedRelationshipsURL      string `json:"all_typed_relationships"`
	AllRelationshipsURL           string `json:"all_relationships"`
	OutgoingTypedRelationshipsURL string `json:"outgoing_typed_relationships"`
	PropertiesURL                 string `json:"properties"`
	IncomingRelationshipsURL      string `json:"incoming_relationships"`
	IncomingTypedRelationshipsURL string `json:"incoming_typed_relationships"`
	CreateRelationshipURL         string `json:"create_relationship"`
}

type Relationship struct {
	relationship
}

func (r Relationship) StartNode() (*Node, error) {
	return r.node("GET", r.StartURL, nil)
}

func (r Relationship) GetProperties() (M, error) {
	return r.properties("GET", r.PropertiesURL, nil)
}

func (r Relationship) SetProperty(p string, v interface{}) error {
	return r.request("PUT", r.PropertiesURL+"/"+p, v, nil)
}

func (r Relationship) SetProperties(p M) error {
	return r.request("PUT", r.PropertiesURL, p, nil)
}

type relationship struct {
	neo4j
	SelfURL       string `json:"self"`
	Type          string `json:"type"`
	StartURL      string `json:"start"`
	PropertyURL   string `json:"property"`
	PropertiesURL string `json:"properties"`
	EndURL        string `json:"end"`
}
