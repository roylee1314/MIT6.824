package main

import "fmt"

type Ball struct {
    Radius   int
    Material string
}

type Bouncer interface {
    Bounce()
}

type Football struct {
    Ball
    Bouncer
}

// compliant to signature of Bouncer interface
func (b Ball) Bounce() {
    fmt.Printf("Bouncing ball %+v\n", b)
}

func main() {
	fb := Football{Ball{Radius: 5, Material: "leather"}}
	//fmt.Printf("fb = %+v\n", fb)
	fb.Bounce()
	//BounceIt(fb)
}
