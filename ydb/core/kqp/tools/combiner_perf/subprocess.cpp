#include "subprocess.h"

#include <util/stream/output.h>
#include <util/stream/pipe.h>
#include <util/stream/buffer.h>
#include <util/generic/buffer.h>
#include <sys/resource.h>
#include <sys/wait.h>

TRunResult RunForked(TRunPayload payload)
{
    Cout.Flush();
    Cerr.Flush();

    TPipeHandle sender;
    TPipeHandle receiver;
    TPipeHandle::Pipe(receiver, sender);

    pid_t pid = fork();
    if (pid > 0) {
        int status = 0;
        while (waitpid(pid, &status, 0) < 0 && errno == EINTR) {
        }
        if (status != 0) {
            ythrow yexception() << "A forked test has crashed with exit code " << status;
        }

        sender.Close();

        TPipedInput inf(receiver);
        TRunResult payloadResult;
        payloadResult.Load(&inf);
        return payloadResult;
    }
    else if (pid < 0) {
        ythrow yexception() << "Failed to fork the test: " << errno;
    }

    receiver.Close();
    TRunResult result = payload();
    TPipedOutput outf(sender);
    result.Save(&outf);
    outf.Finish();
    std::exit(0);
}