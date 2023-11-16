package clip

type options struct {
	openBound bool
}

// An Option is a possible parameter to the clip operations.
type Option func(*options)

// OpenBound is an option to treat the bound as open. i.e. any lines
// along the bound sides will be removed and a point on boundary will
// cause the line to be split.
func OpenBound(yes bool) Option {
	return func(o *options) {
		o.openBound = yes
	}
}
