package model

import "time"

type Row struct {
	TimeEvent     time.Time
	Tachometer    float64
	UbaAxial      float64
	UbaRadial     float64
	UbaTangential float64
	ObaAxial      float64
	ObaRadial     float64
	ObaTangential float64
	Microphone    float64
}

type RowAg struct {
	TimeEvent        time.Time
	TachometerAvg    float64
	TachometerMax    float64
	TachometerMin    float64
	UbaAxialAvg      float64
	UbaAxialMax      float64
	UbaAxialMin      float64
	UbaRadialAvg     float64
	UbaRadialMax     float64
	UbaRadialMin     float64
	UbaTangentialAvg float64
	UbaTangentialMax float64
	UbaTangentialMin float64
	ObaAxialAvg      float64
	ObaAxialMax      float64
	ObaAxialMin      float64
	ObaRadialAvg     float64
	ObaRadialMax     float64
	ObaRadialMin     float64
	ObaTangentialAvg float64
	ObaTangentialMax float64
	ObaTangentialMin float64
	MicrophoneAvg    float64
	MicrophoneMax    float64
	MicrophoneMin    float64
}

type TimeRange struct {
	TMin time.Time
	TMax time.Time
}
