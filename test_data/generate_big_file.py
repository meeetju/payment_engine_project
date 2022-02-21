import csv

with open('large.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['type','client','tx','amount'])
    for i in range(30000):
        writer.writerow(['deposit', str(i), str(i), str(0.0001 + i)])