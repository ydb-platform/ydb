-- ValidateUdf off, so that the call takes the IR path this case is here to exercise.
pragma config.flags("ValidateUdf", "None");

SELECT BitcodeProbe::Twice(1.5);
