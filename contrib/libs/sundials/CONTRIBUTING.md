# Contributing to SUNDIALS

At this time, the SUNDIALS team does not have the resources to review and take
in large additions to the code or significant new features. Contributions
addressing bug fixes or minor changes are preferred via a pull request to the
[SUNDIALS GitHub repository](https://github.com/LLNL/sundials).

All new contributions to SUNDIALS must be made under the BSD 3-clause license.
See the [LICENSE](./LICENSE) and [NOTICE](./NOTICE) files for details. The
SUNDIALS team will not accept any file previously released under any other open
source license. By submitting code, the contributor gives irreversible consent
to the redistribution and/or modification of the contributed source code.

Please ensure that any pull request includes user guide additions or changes as
appropriate, has been tested, and includes a test for any added features.

Any added files submitted with a pull request must contain a header section at
the top including the originating author's name and file origin date as well as
a pointer to the SUNDIALS LICENSE and NOTICE files.

The act of submitting a pull request (with or without an explicit Signed-off-by
tag) will be understood as an affirmation of the following [Developer's
Certificate of Origin (DCO)](http://developercertificate.org/).

```
Developer Certificate of Origin
Version 1.1

Copyright (C) 2004, 2006 The Linux Foundation and its contributors.
1 Letterman Drive
Suite D4700
San Francisco, CA, 94129

Everyone is permitted to copy and distribute verbatim copies of this
license document, but changing it is not allowed.


Developer's Certificate of Origin 1.1

By making a contribution to this project, I certify that:

(a) The contribution was created in whole or in part by me and I
    have the right to submit it under the open source license
    indicated in the file; or

(b) The contribution is based upon previous work that, to the best
    of my knowledge, is covered under an appropriate open source
    license and I have the right under that license to submit that
    work with modifications, whether created in whole or in part
    by me, under the same open source license (unless I am
    permitted to submit under a different license), as indicated
    in the file; or

(c) The contribution was provided directly to me by some other
    person who certified (a), (b) or (c) and I have not modified
    it.

(d) I understand and agree that this project and the contribution
    are public and that a record of the contribution (including all
    personal information I submit with it, including my sign-off) is
    maintained indefinitely and may be redistributed consistent with
    this project or the open source license(s) involved.
```

As discussed in the [Docker software project blog](https://blog.docker.com/2014/01/docker-code-contributions-require-developer-certificate-of-origin/)
this DCO "lets us know that you are entitled to contribute this code to
[SUNDIALS] and that you are willing to have it used in distributions and
derivative works."

"By including the DCO signature, you are stating that one or
more of the following is true of your contribution:

1.  You created this contribution/change and have the right to submit it
    to SUNDIALS; or
2.  You created this contribution/change based on a previous work with a
    compatible open source license; or
3.  This contribution/change has been provided to you by someone who did
    1 or 2 and you are submitting the contribution unchanged.
4.  You understand this contribution is public and may be redistributed as
    open source software" under the BSD license.

All commits submitted to the SUNDIALS project need to have the following sign
off line in the commit message:
```
Signed-off-by: Jane Doe <jdoe@address.com>
```
Replacing Jane Doe’s details with your name and email address.

If you've set `user.name` and `user.email` in your Git configuration, you can
automatically add a sign off line at the end of the commit message by using the
`-s` option (e.g., `git commit -s`).
