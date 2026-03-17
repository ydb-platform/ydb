typedef struct {
    PyObject_HEAD
    key_t key;
    int id;
    short op_flags;
} Semaphore;


/* Object methods */
PyObject *Semaphore_new(PyTypeObject *type, PyObject *, PyObject *);
int Semaphore_init(Semaphore *, PyObject *, PyObject *);
void Semaphore_dealloc(Semaphore *);
PyObject *Semaphore_enter(Semaphore *);
PyObject *Semaphore_exit(Semaphore *, PyObject *);
PyObject *Semaphore_P(Semaphore *, PyObject *, PyObject *);
PyObject *Semaphore_acquire(Semaphore *, PyObject *, PyObject *);
PyObject *Semaphore_V(Semaphore *, PyObject *, PyObject *);
PyObject *Semaphore_release(Semaphore *, PyObject *, PyObject *);
PyObject *Semaphore_Z(Semaphore *, PyObject *, PyObject *);
PyObject *Semaphore_remove(Semaphore *);

/* Object attributes (read-write & read-only) */
PyObject *sem_get_value(Semaphore *);
int sem_set_value(Semaphore *self, PyObject *py_value);

PyObject *sem_get_block(Semaphore *);
int sem_set_block(Semaphore *self, PyObject *py_value);

PyObject *sem_get_mode(Semaphore *);
int sem_set_mode(Semaphore *, PyObject *);

PyObject *sem_get_undo(Semaphore *);
int sem_set_undo(Semaphore *self, PyObject *py_value);

PyObject *sem_get_uid(Semaphore *);
int sem_set_uid(Semaphore *, PyObject *);

PyObject *sem_get_gid(Semaphore *);
int sem_set_gid(Semaphore *, PyObject *);

PyObject *sem_get_key(Semaphore *);
PyObject *sem_get_c_uid(Semaphore *);
PyObject *sem_get_c_gid(Semaphore *);
PyObject *sem_get_last_pid(Semaphore *);
PyObject *sem_get_waiting_for_nonzero(Semaphore *);
PyObject *sem_get_waiting_for_zero(Semaphore *);
PyObject *sem_get_o_time(Semaphore *);

PyObject *sem_str(Semaphore *);
PyObject *sem_repr(Semaphore *);

/* Utility functions */
PyObject *sem_remove(int);
