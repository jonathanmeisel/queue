package worker

type Error interface {
	Retryable() bool
	error
}

type PermanentError struct {
	error
}

func NewPermanentErrorFrom(err error) *PermanentError {
	return &PermanentError{err}
}

func (this *PermanentError) Retryable() bool {
	return false
}

type RetryableError struct {
	error
}

func NewRetryableErrorFrom(err error) *RetryableError {
	return &RetryableError{err}
}

func (this *RetryableError) Retryable() bool {
	return true
}
