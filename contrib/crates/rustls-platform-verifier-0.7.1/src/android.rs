//! On Android, initialization must be done before any verification is attempted.
//!
//! <div class="warning">
//! Some manual setup is required outside of cargo to use this crate on Android. In order to use
//! Android’s certificate verifier, the crate needs to call into the JVM. A small Kotlin component
//! must be included in your app’s build to support rustls-platform-verifier.
//!
//! See the [crate's Android section][crate#android] for more details.
//! </div>
//!
//! # Examples
//!
//! ```
//! // A typical entrypoint signature for obtaining the necessary pointers
//! pub fn android_init(raw_env: *mut c_void, raw_context: *mut c_void) -> Result<(), jni::errors::Error> {
//!     let mut env = unsafe { JNIEnv::from_raw(raw_env as *mut jni::sys::JNIEnv).unwrap() };
//!     let context = unsafe { JObject::from_raw(raw_context as jni::sys::jobject) };
//!     rustls_platform_verifier::android::init_with_env(&mut env, context)?;
//! }
//! ```

use jni::errors::Error as JNIError;
use jni::objects::{Global, JClass, JClassLoader, JObject};
use jni::strings::JNIStr;
use jni::{jni_sig, jni_str, Env, JavaVM};
use once_cell::sync::OnceCell;

static GLOBAL: OnceCell<GlobalStorage> = OnceCell::new();

/// A layer to access the Android runtime which is hosting the current
/// application process.
///
/// Generally this trait should be implemented in your Rust app component's FFI
/// initialization layer.
pub trait Runtime: Send + Sync {
    /// Returns a handle to the current process' JVM.
    fn java_vm(&self) -> &JavaVM;
    /// Returns a reference to the current app's [Context].
    ///
    /// [Context]: <https://developer.android.com/reference/android/content/Context>
    fn context(&self) -> &Global<JObject<'static>>;
    /// Returns a reference to the class returned by the current JVM's `getClassLoader` call.
    fn class_loader(&self) -> &Global<JClassLoader<'static>>;
}

enum GlobalStorage {
    Internal {
        java_vm: JavaVM,
        context: Global<JObject<'static>>,
        loader: Global<JClassLoader<'static>>,
    },
    External(&'static dyn Runtime),
}

impl GlobalStorage {
    fn vm(&self) -> &JavaVM {
        match self {
            GlobalStorage::Internal { java_vm, .. } => java_vm,
            GlobalStorage::External(runtime) => runtime.java_vm(),
        }
    }

    fn context(&self, env: &mut Env) -> Result<GlobalContext, Error> {
        let context = match self {
            Self::Internal { context, .. } => context,
            Self::External(global) => global.context(),
        };

        let loader = match self {
            Self::Internal { loader, .. } => loader,
            Self::External(global) => global.class_loader(),
        };

        Ok(GlobalContext {
            context: env.new_global_ref(context)?,
            loader: env.new_global_ref(loader)?,
        })
    }
}

pub(super) struct GlobalContext {
    /// The Android application [Context](https://developer.android.com/reference/android/app/Application).
    pub(super) context: Global<JObject<'static>>,
    loader: Global<JClassLoader<'static>>,
}

fn global() -> &'static GlobalStorage {
    GLOBAL
        .get()
        .expect("Expect rustls-platform-verifier to be initialized")
}

/// Initialize given a typical Android NDK [`Env`] and [`JObject`] context.
///
/// This method will setup and store an environment locally. This is useful if nothing else in your
/// application needs to access the Android runtime.
pub fn init_with_env(env: &mut Env, context: JObject) -> Result<(), JNIError> {
    GLOBAL.get_or_try_init(|| -> Result<_, JNIError> {
        let loader = env
            .call_method(
                &context,
                jni_str!("getClassLoader"),
                jni_sig!(() -> JClassLoader),
                &[],
            )?
            .l()?;
        let loader = env.cast_local::<JClassLoader>(loader)?;

        Ok(GlobalStorage::Internal {
            java_vm: env.get_java_vm()?,
            context: env.new_global_ref(context)?,
            loader: env.new_global_ref(loader)?,
        })
    })?;
    Ok(())
}

/// Initialize with a runtime that can dynamically serve references to
/// the JVM, context, and class loader.
///
/// This is the most flexible option, and is useful for advanced use cases.
///
/// This function will never panic.
pub fn init_with_runtime(runtime: &'static dyn Runtime) {
    GLOBAL.get_or_init(|| GlobalStorage::External(runtime));
}

/// Initialize with references to the JVM, context, and class loader.
///
/// This is useful when you're already interacting with `jni-rs` wrapped objects and want to use
/// global references to objects for efficiency.
///
/// This function will never panic.
///
/// # Examples
///
/// ```
/// pub fn android_init(raw_env: *mut c_void, raw_context: *mut c_void) -> Result<(), jni::errors::Error> {
///     let mut env = unsafe { jni::EnvUnowned::from_raw(raw_env as *mut jni::sys::JNIEnv).unwrap() };
///     let context = unsafe { JObject::from_raw(raw_context as jni::sys::jobject) };
///     let loader = env.get_object_class(&context)?.get_class_loader(env)?;
///
///     env.with_env(|env| {
///         rustls_platform_verifier::android::init_with_refs(
///             env.get_java_vm(),
///             env.new_global_ref(context)?,
///             env.new_global_ref(loader)?,
///         );
///     });
/// }
/// ```
pub fn init_with_refs(
    java_vm: JavaVM,
    context: Global<JObject<'static>>,
    loader: Global<JClassLoader<'static>>,
) {
    GLOBAL.get_or_init(|| GlobalStorage::Internal {
        java_vm,
        context,
        loader,
    });
}

/// Wrapper for JNI errors that will log and clear exceptions
/// It should generally be preferred to `jni::errors::Error`
#[derive(Debug)]
pub(super) struct Error;

impl From<JNIError> for Error {
    #[track_caller]
    fn from(cause: JNIError) -> Self {
        if let JNIError::JavaException = cause {
            let _ = global()
                .vm()
                .with_top_local_frame(|env| -> Result<(), JNIError> {
                    env.exception_describe();
                    env.exception_clear();
                    Ok(())
                });
        }

        Self
    }
}

pub(super) struct LocalContext<'a, 'env> {
    pub(super) env: &'a mut Env<'env>,
    pub(super) global: GlobalContext,
}

impl<'env> LocalContext<'_, 'env> {
    /// Load a class from the application class loader
    ///
    /// This should be used instead of `JNIEnv::find_class` to ensure all classes
    /// in the application can be found.
    fn load_class(&mut self, name: &'static JNIStr) -> Result<JClass<'env>, Error> {
        let name = self.env.new_string(name.to_str())?;
        self.global
            .loader
            .load_class(self.env, name)
            .map_err(Error::from)
    }
}

/// Borrow the Android application context and execute the closure
/// `with_context, ensuring locals are properly freed and exceptions
/// are cleared.
pub(super) fn with_context<F, T: 'static>(f: F) -> Result<T, Error>
where
    F: FnOnce(&mut LocalContext) -> Result<T, Error>,
{
    let global = global();
    // Use `attach_current_thread_for_scope()` to avoid permanently attaching any threads.
    // See https://github.com/rustls/rustls-platform-verifier/pull/185.
    global.vm().attach_current_thread_for_scope(|env| {
        let global_context = global.context(env)?;
        let mut context = LocalContext {
            env,
            global: global_context,
        };
        f(&mut context)
    })
}

/// Loads and caches a class on first use
pub(super) struct CachedClass {
    name: &'static JNIStr,
    class: OnceCell<Global<JClass<'static>>>,
}

impl CachedClass {
    /// Creates a lazily initialized class reference to the class with `name`.
    pub(super) const fn new(name: &'static JNIStr) -> Self {
        Self {
            name,
            class: OnceCell::new(),
        }
    }

    /// Gets the cached class reference, loaded on first use
    pub(super) fn get(&self, cx: &mut LocalContext) -> Result<&JClass<'static>, Error> {
        let class = self.class.get_or_try_init(|| -> Result<_, Error> {
            let class = cx.load_class(self.name)?;

            Ok(cx.env.new_global_ref(class)?)
        })?;

        Ok(class)
    }
}
