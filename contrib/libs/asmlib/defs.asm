%ifdef UNIX
    %ifdef DARWIN
        %define EXP(x) _ %+ x
    %else
        %define EXP(x) x
    %endif
%else
    %define EXP(x) _ %+ x
    %define WINDOWS
%endif

%define ALLOW_OVERRIDE 1

%ifdef WINDOWS
    %define WEAK_SYM(x) global x
%else
    %ifdef DARWIN
        %define WEAK_SYM(x) global x
    %else
        %define WEAK_SYM(x) weak x
    %endif
%endif
