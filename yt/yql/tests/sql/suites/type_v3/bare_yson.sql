/* custom error: Strict Yson type is not allowed to write, please use Optional<Yson> */
use plato;

pragma yt.UseNativeYtTypes;
pragma yt.NativeYtTypeCompatibility="complex";

select [key] as keys, Yson::From(subkey) as yson_subkey from Input;
