GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v1.9.1)

SRCS(
    active_help.go
    args.go
    bash_completions.go
    bash_completionsV2.go
    cobra.go
    command.go
    completions.go
    fish_completions.go
    flag_groups.go
    powershell_completions.go
    shell_completions.go
    zsh_completions.go
)

GO_TEST_SRCS(
    active_help_test.go
    args_test.go
    bash_completionsV2_test.go
    bash_completions_test.go
    cobra_test.go
    command_test.go
    completions_test.go
    fish_completions_test.go
    flag_groups_test.go
    powershell_completions_test.go
    zsh_completions_test.go
)

IF (OS_LINUX)
    SRCS(
        command_notwin.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        command_notwin.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        command_win.go
    )
ENDIF()

END()

RECURSE(
    #cobra disabled because of dependency on viper
    #doc disabled because of broken dependency
    gotest
)
