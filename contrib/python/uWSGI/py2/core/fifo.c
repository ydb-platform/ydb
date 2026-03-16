#include <contrib/python/uWSGI/py2/config.h>
#include "uwsgi.h"

extern struct uwsgi_server uwsgi;

/*

	the --master-fifo option create a unix named pipe (fifo) you can use to send management
	commands to the master:

	echo r > myfifo

*/

// this var can be accessed by plugins and hooks
void (*uwsgi_fifo_table[256])(int);

static char *uwsgi_fifo_by_slot() {
	int count = 0;
	struct uwsgi_string_list *usl;
	uwsgi_foreach(usl, uwsgi.master_fifo) {
		if (count == uwsgi.master_fifo_slot) return usl->value;
		count++;
	}
	return uwsgi.master_fifo->value;
}

#define announce_fifo uwsgi_log_verbose("active master fifo is now %s\n", uwsgi_fifo_by_slot())

static void uwsgi_fifo_set_slot_zero(int signum) { uwsgi.master_fifo_slot = 0; announce_fifo; }
static void uwsgi_fifo_set_slot_one(int signum) { uwsgi.master_fifo_slot = 1; announce_fifo; }
static void uwsgi_fifo_set_slot_two(int signum) { uwsgi.master_fifo_slot = 2; announce_fifo; }
static void uwsgi_fifo_set_slot_three(int signum) { uwsgi.master_fifo_slot = 3; announce_fifo; }
static void uwsgi_fifo_set_slot_four(int signum) { uwsgi.master_fifo_slot = 4; announce_fifo; }
static void uwsgi_fifo_set_slot_five(int signum) { uwsgi.master_fifo_slot = 5; announce_fifo; }
static void uwsgi_fifo_set_slot_six(int signum) { uwsgi.master_fifo_slot = 6; announce_fifo; }
static void uwsgi_fifo_set_slot_seven(int signum) { uwsgi.master_fifo_slot = 7; announce_fifo; }
static void uwsgi_fifo_set_slot_eight(int signum) { uwsgi.master_fifo_slot = 8; announce_fifo; }
static void uwsgi_fifo_set_slot_nine(int signum) { uwsgi.master_fifo_slot = 9; announce_fifo; }

static void subscriptions_blocker(int signum) {
	if (uwsgi.subscriptions_blocked) {
		uwsgi_log_verbose("subscriptions re-enabled\n");
		uwsgi.subscriptions_blocked = 0;
	}
	else {
		uwsgi.subscriptions_blocked = 1;
		uwsgi_log_verbose("subscriptions blocked\n");
	}
}

static void emperor_rescan(int signum) {
	if (uwsgi.emperor_pid > 0) {
		if (kill(uwsgi.emperor_pid, SIGWINCH)) {
			uwsgi_error("emperor_rescan()/kill()");
		}
	}
}

/*

this is called as soon as possibile allowing plugins (or hooks) to override it

*/
void uwsgi_master_fifo_prepare() {
	int i;
	for(i=0;i<256;i++) {
		uwsgi_fifo_table[i] = NULL;
	}

	uwsgi_fifo_table['0'] = uwsgi_fifo_set_slot_zero;
	uwsgi_fifo_table['1'] = uwsgi_fifo_set_slot_one;
	uwsgi_fifo_table['2'] = uwsgi_fifo_set_slot_two;
	uwsgi_fifo_table['3'] = uwsgi_fifo_set_slot_three;
	uwsgi_fifo_table['4'] = uwsgi_fifo_set_slot_four;
	uwsgi_fifo_table['5'] = uwsgi_fifo_set_slot_five;
	uwsgi_fifo_table['6'] = uwsgi_fifo_set_slot_six;
	uwsgi_fifo_table['7'] = uwsgi_fifo_set_slot_seven;
	uwsgi_fifo_table['8'] = uwsgi_fifo_set_slot_eight;
	uwsgi_fifo_table['9'] = uwsgi_fifo_set_slot_nine;

	uwsgi_fifo_table['-'] = (void (*)(int))uwsgi_cheaper_decrease;
	uwsgi_fifo_table['+'] = (void (*)(int))uwsgi_cheaper_increase;
	uwsgi_fifo_table['B'] = (void (*)(int))vassal_sos;
	uwsgi_fifo_table['c'] = (void (*)(int))uwsgi_chain_reload;
	uwsgi_fifo_table['C'] = (void (*)(int))uwsgi_go_cheap;
	uwsgi_fifo_table['E'] = emperor_rescan;
	uwsgi_fifo_table['f'] = (void (*)(int))uwsgi_refork_master;
	uwsgi_fifo_table['l'] = (void (*)(int))uwsgi_log_reopen;
	uwsgi_fifo_table['L'] = (void (*)(int))uwsgi_log_rotate;
	uwsgi_fifo_table['p'] = suspend_resume_them_all;
	uwsgi_fifo_table['P'] = (void (*)(int))uwsgi_update_pidfiles;
	uwsgi_fifo_table['q'] = gracefully_kill_them_all;
	uwsgi_fifo_table['Q'] = kill_them_all;
	uwsgi_fifo_table['r'] = grace_them_all;
	uwsgi_fifo_table['R'] = reap_them_all;
	uwsgi_fifo_table['s'] = stats;
	uwsgi_fifo_table['S'] = subscriptions_blocker;
	uwsgi_fifo_table['w'] = (void (*)(int))uwsgi_reload_workers;
	uwsgi_fifo_table['W'] = (void (*)(int))uwsgi_brutally_reload_workers;

}

int uwsgi_master_fifo() {

	char *path = uwsgi_fifo_by_slot();

	if (unlink(path) != 0 && errno != ENOENT) {
		uwsgi_error("uwsgi_master_fifo()/unlink()");
	}

	if (mkfifo(path, S_IRUSR|S_IWUSR)) {
		uwsgi_error("uwsgi_master_fifo()/mkfifo()");
		exit(1);
	}

	int fd = open(path, O_RDONLY|O_NONBLOCK);
	if (fd < 0) {
		uwsgi_error("uwsgi_master_fifo()/open()");
		exit(1);
	}

	uwsgi_socket_nb(fd);

	return fd;
}

int uwsgi_master_fifo_manage(int fd) {
	unsigned char cmd;
	ssize_t rlen = read(fd, &cmd, 1);
	if (rlen < 0) {
		if (uwsgi_is_again()) return 0;
		uwsgi_error("uwsgi_master_fifo_manage()/read()");
		exit(1);
	}
	// fifo destroyed, recreate it
	else if (rlen == 0) {
		event_queue_del_fd(uwsgi.master_queue, uwsgi.master_fifo_fd, event_queue_read());
		close(fd);
		uwsgi.master_fifo_fd = uwsgi_master_fifo();
		event_queue_add_fd_read(uwsgi.master_queue, uwsgi.master_fifo_fd);
		return 0;
	}

	if (uwsgi_fifo_table[(int) cmd]) {
		uwsgi_fifo_table[(int) cmd](0);
	}
	
	return 0;
}
