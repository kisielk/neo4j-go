package neo4j

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
)

type Neo4j struct {
	SelfURL string `json:"self,omitempty"`
	Client  *http.Client
}

type Neo4jError struct {
	Message    string `json:"message"`
	Exception  string `json:"exception"`
	Stacktrace string `json:"stacktrace"`
}

func (e *Neo4jError) Error() string {
	return e.Exception + ":" + e.Message
}

type ServiceRoot struct {
	Neo4j
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

type DataMap map[string]interface{}

type Node struct {
	Neo4j
	SelfURL                       string  `json:"self"`
	Data                          DataMap `json:"data"`
	PagedTraverseURL              string  `json:"paged_traverse"`
	OutgoingRelationshipsURL      string  `json:"outgoing_relationships"`
	TraverseURL                   string  `json:"traverse"`
	AllTypedRelationshipsURL      string  `json:"all_typed_relationships"`
	AllRelationshipsURL           string  `json:"all_relationships"`
	OutgoingTypedRelationshipsURL string  `json:"outgoing_typed_relationships"`
	PropertiesURL                 string  `json:"properties"`
	IncomingRelationshipsURL      string  `json:"incoming_relationships"`
	IncomingTypedRelationshipsURL string  `json:"incoming_typed_relationships"`
	CreateRelationshipURL         string  `json:"create_relationship"`
}

type Relationship struct {
	Neo4j
	SelfURL       string  `json:"self"`
	Data          DataMap `json:"data"`
	Type          string  `json:"type"`
	StartURL      string  `json:"start"`
	PropertyURL   string  `json:"property"`
	PropertiesURL string  `json:"properties"`
	EndURL        string  `json:"end"`
}

const DEFAULT_NEO4J_ADDR = "http://localhost:7474/db/data/"

func NewServiceRoot(addr string) (*ServiceRoot, error) {
	if len(addr) == 0 {
		addr = DEFAULT_NEO4J_ADDR
	}
	n := new(ServiceRoot)
	n.SelfURL = addr
	n.Client = &http.Client{}

	r, err := n.neo4jRequest("GET", n.SelfURL, nil)
	if err != nil {
		return nil, err
	}

	d := json.NewDecoder(r)

	err = d.Decode(&n)
	if err != nil {
		return nil, err
	}

	return n, nil
}

func (n ServiceRoot) RelationshipTypes() ([]string, error) {
	r, err := n.neo4jRequest("GET", n.RelationshipTypesURL, nil)
	if err != nil {
		return nil, err
	}

	var result []string
	d := json.NewDecoder(r)
	err = d.Decode(&r)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (n ServiceRoot) CreateNode(properties DataMap) (*Node, error) {
	r, err := n.neo4jRequest("PUT", n.NodeURL, properties)
	if err != nil {
		return nil, err
	}

	return decodeNode(r)
}

func (n ServiceRoot) GetNode(id uint) (*Node, error) {
	r, err := n.neo4jRequest("GET", n.NodeURL+"/"+string(id), nil)
	if err != nil {
		return nil, err
	}

	return decodeNode(r)
}

func (n ServiceRoot) DeleteNode(id uint) error {
	_, err := n.neo4jRequest("DELETE", n.NodeURL+"/"+string(id), nil)
	return err
}

func (n ServiceRoot) GetRelationship(id uint) (*Relationship, error) {
	// Wait till https://github.com/neo4j/community/issues/750 is resolved
	/*
		r, err := n.neo4jRequest("GET", n.RelationshipURL, nil)
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

func (n Node) GetProperties() (*DataMap, error) {
	r, err := n.neo4jRequest("GET", n.PropertiesURL, nil)
	if err != nil {
		return nil, err
	}
	return decodeProperties(r)
}

func (n Node) SetProperty(p string, v interface{}) error {
	_, err := n.neo4jRequest("PUT", n.PropertiesURL+"/"+p, v)
	return err
}

func (n Node) SetProperties(p DataMap) error {
	_, err := n.neo4jRequest("PUT", n.PropertiesURL, p)
	return err
}

func (n Node) DeleteProperties() error {
	_, err := n.neo4jRequest("DELETE", n.PropertiesURL, nil)
	return err
}

func (n Node) DeleteProperty(p string) error {
	_, err := n.neo4jRequest("DELETE", n.PropertiesURL+"/"+p, nil)
	return err
}

func (n Node) CreateRelationship(to *Node, rtype string, properties DataMap) (*Relationship, error) {
	args := make(DataMap)
	args["to"] = to.SelfURL
	args["type"] = rtype
	args["data"] = properties
	r, err := n.neo4jRequest("POST", n.CreateRelationshipURL, args)
	if err != nil {
		return nil, err
	}

	return decodeRelationship(r)
}

func (n Node) getRelationships(url string) (*[]Relationship, error) {
	r, err := n.neo4jRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	return decodeRelationships(r)
}

func (n Node) GetIncomingRelationships() (*[]Relationship, error) {
	return n.getRelationships(n.IncomingRelationshipsURL)
}

func (n Node) GetOutgoingRelationships() (*[]Relationship, error) {
	return n.getRelationships(n.OutgoingRelationshipsURL)
}

func (n Node) GetAllRelationships() (*[]Relationship, error) {
	return n.getRelationships(n.AllRelationshipsURL)
}

func (n Node) GetTypedRelationships(types []string) (*[]Relationship, error) {
	return n.getRelationships(n.AllRelationshipsURL + "/" + strings.Join(types, "%26")) // %26 = &
}

func (r Relationship) StartNode() (*Node, error) {
	resp, err := r.neo4jRequest("GET", r.StartURL, nil)
	if err != nil {
		return nil, err
	}
	return decodeNode(resp)
}

func (r Relationship) GetProperties() (*DataMap, error) {
	resp, err := r.neo4jRequest("GET", r.PropertiesURL, nil)
	if err != nil {
		return nil, err
	}
	return decodeProperties(resp)
}

func (r Relationship) SetProperty(p string, v interface{}) error {
	_, err := r.neo4jRequest("PUT", r.PropertiesURL+"/"+p, v)
	return err
}

func (r Relationship) SetProperties(p DataMap) error {
	_, err := r.neo4jRequest("PUT", r.PropertiesURL, p)
	return err
}

func decodeProperties(r io.Reader) (*DataMap, error) {
	d := json.NewDecoder(r)
	p := new(DataMap)
	err := d.Decode(&p)
	if err != nil {
		return nil, err
	}

	return p, nil
}

func decodeError(r io.Reader) error {
	d := json.NewDecoder(r)
	neo4jError := new(Neo4jError)
	err := d.Decode(&neo4jError)
	if err != nil {
		return err
	}
	return neo4jError
}

func decodeNode(r io.Reader) (*Node, error) {
	d := json.NewDecoder(r)
	node := new(Node)
	err := d.Decode(&node)
	if err != nil {
		return nil, err
	}

	return node, nil
}

func decodeRelationship(r io.Reader) (*Relationship, error) {
	d := json.NewDecoder(r)
	relationship := new(Relationship)
	err := d.Decode(&relationship)
	if err != nil {
		return nil, err
	}

	return relationship, nil
}

func decodeRelationships(r io.Reader) (*[]Relationship, error) {
	d := json.NewDecoder(r)
	relationships := new([]Relationship)
	err := d.Decode(&relationships)
	if err != nil {
		return nil, err
	}

	return relationships, nil
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

func (n Neo4j) neo4jRequest(method string, urlStr string, body interface{}) (io.Reader, error) {
	marshalledBody, err := marshallBody(body)
	if err != nil {
		return nil, err
	}

	r, err := http.NewRequest(method, urlStr, marshalledBody)
	if err != nil {
		return nil, err
	}

	r.Header.Add("Accept", "application/json")
	if method == "PUT" {
		r.Header.Add("Content-Type", "application/json")
	}

	resp, err := n.Client.Do(r)

	buf := new(bytes.Buffer)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	_, err = buf.ReadFrom(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode >= 400 && resp.StatusCode < 500 {
		err := decodeError(buf)
		return nil, err
	}

	return buf, nil
}
