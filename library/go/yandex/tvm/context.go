package tvm

import "context"

type (
	serviceTicketContextKey struct{}
	userTicketContextKey    struct{}
)

var (
	stKey serviceTicketContextKey
	utKey userTicketContextKey
)

// WithServiceTicket returns copy of the ctx with service ticket attached to it.
func WithServiceTicket(ctx context.Context, t *CheckedServiceTicket) context.Context {
	return context.WithValue(ctx, &stKey, t)
}

// WithUserTicket returns copy of the ctx with user ticket attached to it.
func WithUserTicket(ctx context.Context, t *CheckedUserTicket) context.Context {
	return context.WithValue(ctx, &utKey, t)
}

func ContextServiceTicket(ctx context.Context) (t *CheckedServiceTicket) {
	t, _ = ctx.Value(&stKey).(*CheckedServiceTicket)
	return
}

func ContextUserTicket(ctx context.Context) (t *CheckedUserTicket) {
	t, _ = ctx.Value(&utKey).(*CheckedUserTicket)
	return
}
