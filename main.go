package main

func main() {

	type thing struct {
		ID       int64  `json:"id"`
		Contents string `json:"contents"`
	}

	t := thing{
		ID:       123,
		Contents: "what's up doc?",
	}

	chunker.Upsert(t.ID, t)

}
