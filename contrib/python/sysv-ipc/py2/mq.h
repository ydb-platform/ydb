#include <limits.h>  // for definition of SSIZE_MAX

typedef struct {
    PyObject_HEAD
    key_t key;
    int id;
    unsigned long max_message_size;
} MessageQueue;

/* Message queue message struct for send() & receive()
On many systems this is defined in sys/msg.h already, but it's better
for me to define it here. Name it something other than msgbuf to avoid
conflict with the struct that the OS header file might define.
*/
struct queue_message {
    long type;
    char message[];
};

/* Maximum message size is limited by (a) the largest Python string I can
create and (b) SSIZE_MAX. The latter restriction comes from the spec which
says, "If the value of msgsz is greater than {SSIZE_MAX}, the result is
implementation-defined."
ref: http://www.opengroup.org/onlinepubs/000095399/functions/msgrcv.html
*/
#define MIN(a,b) (((a)<(b))?(a):(b))
#define QUEUE_MESSAGE_SIZE_MAX MIN(SSIZE_MAX, PY_STRING_LENGTH_MAX)

/* The max message size is probably a very big number, and since a
max-sized buffer is allocated every time receive() is called, it would be
ugly if the default message size for new queues was the same as the max.
In addition, many operating systems limit the entire queue to 2048 bytes,
so defaulting the max message to something larger seems a bit stupid.

This value is also present in numeric form in ReadMe.html, so if you
change it here, change it there too.
*/
#define QUEUE_MESSAGE_SIZE_MAX_DEFAULT 2048

/* Object methods */
PyObject *MessageQueue_new(PyTypeObject *, PyObject *, PyObject *);
int MessageQueue_init(MessageQueue *, PyObject *, PyObject *);
void MessageQueue_dealloc(MessageQueue *);
PyObject *MessageQueue_send(MessageQueue *, PyObject *, PyObject *);
PyObject *MessageQueue_receive(MessageQueue *, PyObject *, PyObject *);
PyObject *MessageQueue_remove(MessageQueue *);

/* Object attributes (read-write & read-only) */
PyObject *mq_get_mode(MessageQueue *);
int mq_set_mode(MessageQueue *, PyObject *);

PyObject *mq_get_uid(MessageQueue *);
int mq_set_uid(MessageQueue *, PyObject *);

PyObject *mq_get_gid(MessageQueue *);
int mq_set_gid(MessageQueue *, PyObject *);

PyObject *mq_get_max_size(MessageQueue *);
int mq_set_max_size(MessageQueue *, PyObject *);

PyObject *mq_get_key(MessageQueue *);
PyObject *mq_get_last_send_time(MessageQueue *);
PyObject *mq_get_last_receive_time(MessageQueue *);
PyObject *mq_get_last_change_time(MessageQueue *);
PyObject *mq_get_last_send_pid(MessageQueue *);
PyObject *mq_get_last_receive_pid(MessageQueue *);
PyObject *mq_get_current_messages(MessageQueue *);
PyObject *mq_get_c_uid(MessageQueue *);
PyObject *mq_get_c_gid(MessageQueue *);

PyObject *mq_str(MessageQueue *);
PyObject *mq_repr(MessageQueue *);

/* Misc. */
PyObject *mq_remove(int);

