#!/usr/bin/env python3

import sys

if len(sys.argv) < 4:
    print("./check.py testF answerF linesToIgnore")
    exit(1)
#txt1="testshmem1_c.txt"
#txt2="answer1.txt"
txt1 = sys.argv[1]
txt2 = sys.argv[2]
lines = int(sys.argv[3])

pre=[]
a=[]
b=[]
t=0.0001

with open(txt1,'r',encoding='utf-16') as file:
    for line in file:
        data=line.split()
        pre.append(data)

a = pre[lines:]

with open(txt2,'r',encoding='utf-16') as file2:
    for line2 in file2:
        data2=line2.split()
        b.append(data2) 

dict1 ={}
dict2 ={}
compare1 =0
compare2 =0
for i in range(2, len(a)):
    if a[i][0] != "compareTreeTime:" :
        dict1[a[i][0]] = sorted(a[i][1:])
    else:
        compare1 = i
        break

for i in range(2, len(b)):
    if b[i][0] != "compareTreeTime:" :
        dict2[b[i][0]] = sorted(b[i][1:])
    else:
        compare2 = i
        break    
#print(dict1)
#print(dict2)

if dict1!=dict2:
    print("hash not same\n")

if len(a) != len(b):
    print("not same")
for i in range(compare1+1,len(a)):
    if len(a[i])!=len(b[i]):
        #print("not same\n")
        print("%s vs %s" % (a[i][0],b[i][0]))
        break
    for j in range(0,len(a[i])):
        if a[i][j]!=b[i][j]:
            print("%s vs %s" % (a[i][j],b[i][j]))