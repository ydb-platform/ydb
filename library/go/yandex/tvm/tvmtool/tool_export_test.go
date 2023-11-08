package tvmtool

func (c *Client) BaseURI() string {
	return c.baseURI
}

func (c *Client) AuthToken() string {
	return c.authToken
}
