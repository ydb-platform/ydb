package httppuller

type spackOption struct {
}

func (*spackOption) isOption() {}

func WithSpack() Option {
	return &spackOption{}
}
