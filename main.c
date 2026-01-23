#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>

#define NUM_TRADERS 3
#define BUFFER_SIZE 10
#define INITIAL_BALANCE 10000.0

// TODO: Definiáld a Transaction struktúrát (láncolt lista)
// Tartalmazzon: type, stock, quantity, price, next pointer
typedef struct Transaction {
    char type[10];
    char stock[10];
    int quantity;
    double price;
    struct Transaction *next;
} Transaction;

// TODO: Definiáld a StockPrice struktúrát
// Tartalmazzon: stock név, price
typedef struct StockPrice {
    char stock[10];
    double price;
} StockPrice;

// TODO: Globális változók
// - price_buffer tömb
// - buffer_count, buffer_read_idx, buffer_write_idx
// - wallet_balance, stocks_owned
// - mutex-ek (wallet, buffer, transaction)
// - condition variable
// - transaction_head pointer
// - running flag (volatile sig_atomic_t)
// - market_pid
StockPrice price_buffer[BUFFER_SIZE];
int buffer_count = 0;
int buffer_read_idx = 0;
int buffer_write_idx = 0;
double wallet_balance = INITIAL_BALANCE;
int stocks_owned = 0;
pthread_mutex_t buffer_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t wallet_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t list_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t data_ready = PTHREAD_COND_INITIALIZER;
Transaction *transaction_head = NULL;
volatile sig_atomic_t running = 1;
pid_t market_pid;

// TODO: Implementáld az add_transaction függvényt
// malloc-al foglalj memóriát, töltsd ki a mezőket
// mutex lock alatt add hozzá a láncolt lista elejéhez
void add_transaction(const char* type, const char* stock, int qty, double price) {
    Transaction *new_node = (Transaction*)malloc(sizeof(Transaction));
    if (new_node) {
        strncpy(new_node->type, type, 10);
        strncpy(new_node->stock, stock, 10);
        new_node->quantity = qty;
        new_node->price = price;
        pthread_mutex_lock(&list_mutex);
        new_node->next = transaction_head;
        transaction_head = new_node;
        pthread_mutex_unlock(&list_mutex);
    }
}

// TODO: Implementáld a print_transactions függvényt
// Járd végig a láncolt listát mutex lock alatt
// Írd ki az összes tranzakciót
void print_transactions() {
    pthread_mutex_lock(&list_mutex);
    printf("\n--- TRANZAKCIÓS NAPLÓ ---\n");
    Transaction *curr = transaction_head;
    while (curr) {
        printf("[%s] %d db %s @ %.2f $\n", curr->type, curr->quantity, curr->stock, curr->price);
        curr = curr->next;
    }
    pthread_mutex_unlock(&list_mutex);
}

// TODO: Implementáld a free_transactions függvényt
// FONTOS: Járd végig a listát és free()-zd az összes elemet
// Ez kell a Valgrind tiszta kimenethez!
void free_transactions() {
    Transaction *curr = transaction_head;
    while (curr) {
        Transaction *temp = curr;
        curr = curr->next;
        free(temp);
    }
    transaction_head = NULL;
}

// TODO: Signal handler (SIGINT)
// Állítsd be a running flag-et 0-ra
// Küldj SIGTERM-et a market_pid folyamatnak (kill függvény)
// Ébreszd fel a szálakat (pthread_cond_broadcast)
void sigint_handler(int sig) {
    (void)sig;
    running = 0;
    if (market_pid > 0) {
        kill(market_pid, SIGTERM);
    }
    pthread_cond_broadcast(&data_ready);
}

// TODO: Piac folyamat függvénye
// Végtelen ciklusban:
// - Generálj random részvénynevet és árat
// - Írás a pipe_fd-re (write)
// - sleep(1)
void market_process(int pipe_write_fd) {
    const char *stocks[] = {"AAPL", "GOOG", "MSFT", "AMZN", "TSLA"};
    srand(time(NULL) ^ getpid());
    while (1) {
        char msg[64];
        const char *stock = stocks[rand() % 5];
        double price = (rand() % 5000) / 10.0 + 100.0;
        int len = sprintf(msg, "%s %.2f\n", stock, price);
        if (write(pipe_write_fd, msg, len) == -1) break;
        sleep(1);
    }
    close(pipe_write_fd);
    exit(0);
}

// TODO: Kereskedő szál függvénye
// Végtelen ciklusban:
// - pthread_cond_wait amíg buffer_count == 0
// - Kivesz egy árfolyamot a bufferből (mutex alatt!)
// - Kereskedési döntés (random vagy stratégia)
// - wallet_balance módosítása (MUTEX!!!)
// - add_transaction hívás
void* trader_thread(void* arg) {
    int id = *((int*)arg);
    free(arg);
    while (running) {
        StockPrice sp;
        pthread_mutex_lock(&buffer_mutex);
        while (buffer_count == 0 && running) {
            pthread_cond_wait(&data_ready, &buffer_mutex);
        }
        if (!running && buffer_count == 0) {
            pthread_mutex_unlock(&buffer_mutex);
            break;
        }
        sp = price_buffer[buffer_read_idx];
        buffer_read_idx = (buffer_read_idx + 1) % BUFFER_SIZE;
        buffer_count--;
        pthread_mutex_unlock(&buffer_mutex);

        if (sp.price < 350.0) {
            pthread_mutex_lock(&wallet_mutex);
            if (wallet_balance >= sp.price) {
                wallet_balance -= sp.price;
                stocks_owned++;
                printf("[TRADER #%d] VÉTEL: %s @ %.2f | Egyenleg: %.2f\n", id, sp.stock, sp.price, wallet_balance);
                add_transaction("BUY", sp.stock, 1, sp.price);
            }
            pthread_mutex_unlock(&wallet_mutex);
        }
    }
    return NULL;
}

int main() {
    int pipe_fd[2];
    pthread_t traders[NUM_TRADERS];
    
    printf("========================================\n");
    printf("  WALL STREET - PARHUZAMOS TOZSDE\n");
    printf("========================================\n");
    printf("Kezdo egyenleg: %.2f $\n", INITIAL_BALANCE);
    printf("Kereskedok szama: %d\n", NUM_TRADERS);
    printf("Ctrl+C a leallitashoz\n");
    printf("========================================\n\n");
    
    // TODO: Signal handler regisztrálása
    // signal(SIGINT, ...);
    signal(SIGINT, sigint_handler);
    
    // TODO: Pipe létrehozása
    // pipe(pipe_fd);
    if (pipe(pipe_fd) == -1) return 1;
    
    // TODO: Fork - Piac folyamat indítása
    // market_pid = fork();
    // Ha gyerek (== 0): piac folyamat
    // Ha szülő: kereskedő szálak indítása
    market_pid = fork();
    if (market_pid < 0) return 1;

    if (market_pid == 0) {
        close(pipe_fd[0]);
        market_process(pipe_fd[1]);
    } else {
        close(pipe_fd[1]);

        // TODO: Kereskedő szálak indítása (pthread_create)
        // for ciklus, malloc az id-nak
        for (int i = 0; i < NUM_TRADERS; i++) {
            int *id = malloc(sizeof(int));
            *id = i + 1;
            pthread_create(&traders[i], NULL, trader_thread, id);
        }
        
        // TODO: Master ciklus
        // Olvasd a pipe-ot (read)
        // Parse-old az árakat
        // Tedd be a bufferbe (mutex alatt!)
        // pthread_cond_broadcast
        char read_buf[128];
        FILE* stream = fdopen(pipe_fd[0], "r");
        while (running && fgets(read_buf, sizeof(read_buf), stream)) {
            StockPrice sp;
            if (sscanf(read_buf, "%s %lf", sp.stock, &sp.price) == 2) {
                pthread_mutex_lock(&buffer_mutex);
                if (buffer_count < BUFFER_SIZE) {
                    price_buffer[buffer_write_idx] = sp;
                    buffer_write_idx = (buffer_write_idx + 1) % BUFFER_SIZE;
                    buffer_count++;
                    pthread_cond_broadcast(&data_ready);
                }
                pthread_mutex_unlock(&buffer_mutex);
            }
        }
        
        // TODO: Cleanup
        // pthread_join a szálakra
        // waitpid a Piac folyamatra
        // Végső kiírások
        // free_transactions()
        // mutex destroy
        for (int i = 0; i < NUM_TRADERS; i++) {
            pthread_join(traders[i], NULL);
        }
        fclose(stream);
        waitpid(market_pid, NULL, 0);
        print_transactions();
        printf("\nVEGSO EGYENLEG: %.2f $\n", wallet_balance);
        free_transactions();
        pthread_mutex_destroy(&buffer_mutex);
        pthread_mutex_destroy(&wallet_mutex);
        pthread_mutex_destroy(&list_mutex);
        pthread_cond_destroy(&data_ready);
    }
    
    printf("\n[RENDSZER] Sikeres leallitas.\n");
    return 0;
}
