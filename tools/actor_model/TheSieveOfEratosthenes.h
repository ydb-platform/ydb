#ifndef ACTOR_MODEL_THESIEVEOFERATOSTHENES_H
#define ACTOR_MODEL_THESIEVEOFERATOSTHENES_H

class TheSieveOfEratosthenes {
private:
    std::vector<bool> prime;
    int n;
public:
    TheSieveOfEratosthenes() {
        selfResize(10);
    }

    void selfResize(int n){
        n = n + 1;
        prime.resize(n, true);
        prime[0] = prime[1] = false;
        for (int i = 2; i <= n; i++) {
            if (prime[i])
                if(i * 1ll * i <= n)
                    for(int j = i * i; j <=n; j += i)
                        prime[j] = false;
        }
    }

    int getN(){
        return n;
    }

    bool checkPrime(int value){
        return prime[value];
    }
};


#endif //ACTOR_MODEL_THESIEVEOFERATOSTHENES_H
