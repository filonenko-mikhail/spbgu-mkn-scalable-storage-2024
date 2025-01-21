package main

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFiller(t *testing.T) {
	b := [100]byte{}
	zero := byte('0')
	one := byte('1')
	filler(b[:], zero, one)

	assert.Truef(t, slices.Contains(b[:], zero) && slices.Contains(b[:], one), "b не содержит и %c и %c", zero, one)
}
