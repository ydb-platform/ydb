package tvm

type RoleParserOption func(options *rolesParserOptions)
type rolesParserOptions struct {
	UseLightIndex bool
}

func newRolesParserOptions(opts ...RoleParserOption) *rolesParserOptions {
	options := &rolesParserOptions{}

	for _, opt := range opts {
		opt(options)
	}

	return options
}

func WithLightIndex() RoleParserOption {
	return func(options *rolesParserOptions) {
		options.UseLightIndex = true
	}
}
