package sqlol

import (
	"time"
)

type Strategy interface {
	Execute(b *Builder)
}

type TryEqual struct {
	Field string
	Value interface{}
}

func (t TryEqual) Execute(b *Builder) {
	b.TryEqual(t.Field, t.Value)
}

type TryLike struct {
	Field string
	Value string
}

func (t TryLike) Execute(b *Builder) {
	b.TryLike(t.Field, t.Value)
}

type TryMultiLike struct {
	Fields []string
	Value  string
}

func (t TryMultiLike) Execute(b *Builder) {
	b.TryMultiLike(t.Fields, t.Value)
}

type TryIn struct {
	Field  string
	Values interface{}
}

func (t TryIn) Execute(b *Builder) {
	b.TryIn(t.Field, t.Values)
}

type TryTimeRange struct {
	Field     string
	StartTime time.Time
	EndTime   time.Time
}

func (t TryTimeRange) Execute(b *Builder) {
	b.TryTimeRange(t.Field, t.StartTime, t.EndTime)
}

type TryDateRange struct {
	Field     string
	StartDate time.Time
	EndDate   time.Time
}

func (t TryDateRange) Execute(b *Builder) {
	TryTimeRange{
		Field:     t.Field,
		StartTime: t.getStartTime(),
		EndTime:   t.getEndTime(),
	}.Execute(b)
}

func (t TryDateRange) getStartTime() time.Time {
	if t.StartDate.IsZero() {
		return t.StartDate
	}
	startTime, _ := time.Parse(TimeLayout, t.StartDate.Format(DateLayout)+" 00:00:00")
	return startTime
}

func (t TryDateRange) getEndTime() time.Time {
	if t.EndDate.IsZero() {
		return t.EndDate
	}
	endTime, _ := time.Parse(TimeLayout, t.EndDate.Format(DateLayout)+" 23:59:59")
	return endTime
}
