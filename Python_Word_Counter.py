#!/usr/bin/env python
f = open('Shakespeare.txt', 'r')
z = [v for line in f for v in line.split()]
for x in z:
    x = x.isalnum()


def counter():
    frequency = dict()
    for v in z:
        if v in frequency:
            frequency[v] += 1
        else:
            frequency[v] = 1

    return frequency


print(counter())
