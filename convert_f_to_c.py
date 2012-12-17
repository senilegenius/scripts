#!/usr/bin/env python

fahrenheit = raw_input('Give temp in F: ')
f_fahrenheit = float(fahrenheit)

f_celcius = (f_fahrenheit - 32) * 5 / 9

print "Your temp, %sF, in Celcius (rounded to nearest 100th) is: %.2f" % (fahrenheit,round(f_celcius,2))
