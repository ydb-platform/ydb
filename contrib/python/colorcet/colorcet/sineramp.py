"""
Converted from the matlab function: https://www.peterkovesi.com/matlabfns/#colour


SINERAMP  - Generates sine on a ramp colour map test image

The test image consists of a sine wave superimposed on a ramp function The
amplitude of the sine wave is modulated from its full value at the top of the
image to 0 at the bottom.

The image is useful for evaluating the effectiveness of different colour maps.
Ideally the sine wave pattern should be equally discernible over the full
range of the colour map.  In addition, across the bottom of the image, one
should not see any identifiable features as the underlying signal is a smooth
ramp.  In practice many colour maps have uneven perceptual contrast over their
range and often include 'flat spots' of no perceptual contrast that can hide
significant features.

Usage: im = sineramp(sze, amp, wavelen, p)
       im = sineramp;

Arguments:     sze - [rows cols] specifying size of test image.  If a
                     single value is supplied the image is square.
                     Defaults to [256 512];  Note the number of columns is
                     nominal and will be adjusted so that there are an
                     integer number of sine wave cycles across the image.
               amp - Amplitude of sine wave. Defaults to 12.5
           wavelen - Wavelength of sine wave in pixels. Defaults to 8.
                 p - Power to which the linear attenuation of amplitude,
                     from top to bottom, is raised.  For no attenuation use
                     p = 0.  For linear attenuation use a value of 1.  For
                     contrast sensitivity experiments use larger values of
                     p.  The default value is 2.

The ramp function that the sine wave is superimposed on is adjusted slightly
for each row so that each row of the image spans the full data range of 0 to
255.  Thus using a large sine wave amplitude will result in the ramp at the
top of the test image being reduced relative to the slope of the ramp at the
bottom of the image. However, the adjustment ensures that, at the lower edge
of the image, the full colour map is displayed.

To start with try
 >> im = sineramp;

This is equivalent to
 >> im = sineramp((256, 512), 12.5, 8, 2);

View it under 'gray' then try the 'jet', 'hsv', 'hot' etc colour maps.  The
results may cause you some concern!

If you are wishing to evaluate a cyclic colour map, say hsv, it is suggested
that you use the test image generated CIRCLESINERAMP.  However you can use
this function to perform a basic evaluation of a cyclic colour map by
displaying two copies of the SINERAMP test image concatenated side-by-side.

 >> show([sineramp sineramp]), colormap(map_to_be_tested)

However, note that despite there being an integer number of sine wave cycles
across the image and that each row has been adjusted to span the full data
range there will be a slight cyclic discontinuity at the top of the image,
though this is progressively removed as you move down the test image.

See source code comments for more details on the default wavelength and
amplitude.

See also: CIRCLESINERAMP, CHIRPLIN, CHIRPEXP, EQUALISECOLOURMAP, CMAP


The Default Wavelength:
The default wavelength is 8 pixels.  On a computer monitor with a nominal
pixel pitch of 0.25mm this corresponds to a wavelength of 2mm.  With a monitor
viewing distance of 600mm this corresponds to 0.19 degrees of viewing angle or
approximately 5.2 cycles per degree.  This falls within the range of spatial
frequencies (3-7 cycles per degree ) at which most people have maximal
contrast sensitivity to a sine wave grating (this varies with mean luminance).
A wavelength of 8 pixels is also sufficient to provide a reasonable discrete
representation of a sine wave.  The aim is to present a stimulus that is well
matched to the performance of the human visual system so that what we are
primarily evaluating is the colour map's perceptual contrast and not the
visual performance of the viewer.

The Default Amplitude:
This is set at 12.5 so that from peak to trough we have a local feature of
magnitude 25.  This is approximately 10of the 256 levels in a standard
colour map. It is not uncommon for colour maps to have perceptual flat spots
that can hide features of this magnitude.

Copyright (c) 2013-2014 Peter Kovesi
Centre for Exploration Targeting
The University of Western Australia
peter.kovesi at uwa edu au

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

The Software is provided "as is", without warranty of any kind.

July  2013  Original version.
March 2014  Adjustments to make it better for evaluating cyclic colour maps.
June  2014  Default wavelength changed from 10 to 8.
"""
import numpy as np

def sineramp(size=(256, 512), amp=12.5, wavelen=8, p=2):
    if len(size) == 1:
        rows = cols = size
    elif len(size) == 2:
        rows, cols = size
    else:
        raise ValueError('size must be of length 1 or 2')

    # Adjust width of image so that we have an integer number of cycles of
    # the sinewave.  This helps should one be using the test image to
    # evaluate a cyclic colour map.  However you will still see a slight
    # cyclic discontinuity at the top of the image, though this will
    # disappear at the bottom of the test image
    cycles = int(cols / wavelen)
    cols = cycles * wavelen

    # Sine wave
    fx = amp * np.array([np.sin(1/wavelen * 2*np.pi*c) for c in range(cols)])

    # Vertical modulating function
    A = (np.arange(rows, 0, -1)/(rows-1)) ** p
    im_0, im_1 = np.meshgrid(fx, A)
    im = im_0 * im_1

    # Add ramp
    ramp_0, ramp_1 = np.meshgrid(range(cols), range(rows))
    ramp = ramp_0/cols
    im = im + ramp * (255 - 2*amp)

    # Now normalise each row so that it spans the full data range from 0 to 255.
    # This ensures that, at the lower edge of the image, the full colour map is
    # displayed.  It also helps with the evaluation of cyclic colour maps though
    # a small cyclic discontinuity will remain at the top of the test image.
    for r in range(rows):
        im[r, :] = (im[r, :])/im[r, :].max()

    return im * 255
