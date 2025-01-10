t = int(input())

for i in range(t):
    k, l1, r1, l2, r2 = [int(val) for val in input().split()]
    
    current_val = 1
    ans = 0
    # print(min_val, max_val)
    while current_val <= 2 * (10**9):
        n_l1 = l1 * current_val
        n_r1 = r1 * current_val
        # print("**", current_val)
        l = max(n_l1, l2)
        r = min(n_r1, r2)
        r += 1
        if (l <= r):
            if (l % current_val):
                l -= l % current_val
                l += current_val

            if (r % current_val):
                r -= r % current_val
                r += current_val

            d = (r-l) // current_val
            # print("*", l, r, current_val, d)
            ans += d
        
        current_val *= k

    print(ans)
