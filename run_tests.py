#!/usr/bin/env python3
import os
from subprocess import check_output
import re
from time import sleep
#import matplotlib

#matplotlib.use('Agg')
#import matplotlib.pyplot as plt

#
# Before running, a dir named results has to be manually created to save output files.
# 

hash_workers = [1]
data_workers = [1]
comp_workers = [0]
hashTimes = []
groupTimes = []
compTimes =[]
hashTime = 0.0
groupTime = 0.0
compTime = 0.0
LOOPS = 10
INPUTS = ["coarse.txt"]

csvs = []
for inp in INPUTS:
    #input = inp.split(".")[0]
    for hash in hash_workers:
        for data in data_workers:
            for comp in comp_workers:   
                #csv = ["{}/{}".format(inp, loop)]
                hashTime = 0.0
                groupTime = 0.0
                compTime = 0.0
                for i in range(0,LOOPS):                                        
                    #save the output in txt files. 
                    #outputName = "%s_p.txt" % (input)
                    cmd = "go run BST_Comp.go -input {} -hash-workers={} -data-workers={} -comp-workers={}".format(
                        inp, hash, data, comp)
                    out = check_output(cmd, shell=True).decode("ascii")
                    m = re.search("hashTime: (.*)", out)
                    #print(out)
                    if m is not None:
                        time = m.group(1)
                        hashTime += float(time)
                    m = re.search("hashGroupTime: (.*)", out)
                    if m is not None:
                        time = m.group(1)
                        groupTime += float(time)
                    m = re.search("compareTreeTime: (.*)", out)
                    if m is not None:
                        time = m.group(1)
                        compTime += float(time)
                
            #compareName= "%s_r.txt" % input
            #cmd = "python check.py {} {} 0".format(compareName, outputName)
            #out = check_output(cmd, shell=True).decode("ascii")
            #print(out)

                    #csvs.append(csv)
                    sleep(0.5)
                hashTimes.append(hashTime/LOOPS)
                groupTimes.append(groupTime/LOOPS)
                compTimes.append(compTime/LOOPS)

print("hashTime:")
print(hashTimes)
print("groupTime:")
print(groupTimes)
print("compTime:")
print(compTimes)
'''
header = ["microseconds"] + [str(x) for x in THREADS]

print("\n")
print(", ".join(header))
for csv in csvs:
    print (", ".join(csv))

#plot and save
plt.ticklabel_format(style='sci',scilimits=(0,0),axis='y')
for csv in csvs:
    plt.plot(THREADS, list(map(int,csv[1:])), 'o-', label = csv[0])

plt.xlabel("Number of threads")
plt.ylabel("Run time in microseconds")
plt.legend()
plt.savefig('10loop8k_s.png')
'''