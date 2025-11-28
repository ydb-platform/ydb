## Add new cases into test_tsconfig_real_files

If there is a new project that suffers from leaked inputs:

1. `cd` to the project directory
2. run `tsc --showConfig > tsconfig.final.json`
    - if your project uses custom tsconfig then `tsc --showConfig -p tsconfig.my.json > tsconfig.final.json`
3. move `tsconfig.final.json` to `build/plugins/lib/nots/typescript/tests/test-data/tsconfig-real-files/tsconfig.<support-ticket>.json`
4. run test to insure it fails

### How test_tsconfig_real_files works

`tsc --showConfig` fills `files` field - list of all discovered files.

The test ensures that with the same config our logic matches all these files. Yes, our logic can match more files, but here we only check that we don't leak required inputs.

