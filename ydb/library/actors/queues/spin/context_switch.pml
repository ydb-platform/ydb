


#define JUMP_POINT(name) \
jump_point_ ## name: \
    goto switching_gate_ ## name; \
// end of JUMP_POINT

#define RETURN_TO_GATE(name) \
    goto switching_gate_ ## name; \
// end of RETURN_TO_GATE


#define SWITH_CONTEXT(name) \
    goto jump_point_ ## name \
// end of SWITH_CONTEXT

#define SWITHING_GATE(name) \
switching_gate_ ## name: \
    skip; \
// end of SWITHING_GATE
