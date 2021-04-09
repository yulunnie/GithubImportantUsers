def main():
    k = 27450   # total is 37700
    f = open("/Users/Yulun/Downloads/GithubImportantUsers/input/edges.csv")
    edges = 0
    small_edges = 0
    for line in f:
        pair = line.split(',')
        t = (int(pair[0]), int(pair[1]))
        edges += 1
        if (t[0] < k and t[1] < k):
            small_edges += 1
    f.close()

    print("Full  dataset edges:", edges)
    print("Small dataset edges:", small_edges)

main()