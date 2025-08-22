# Changelog

## [11.9.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v11.8.0...v11.9.0) (2025-08-21)


### Features

* enable cluster events tab ([#2710](https://github.com/ydb-platform/ydb-embedded-ui/issues/2710)) ([2e3fb59](https://github.com/ydb-platform/ydb-embedded-ui/commit/2e3fb5914a8d994962af13ca33983eb2deddebc0))


### Bug Fixes

* **EntityStatus:** fix buttons bg in selected table rows ([#2738](https://github.com/ydb-platform/ydb-embedded-ui/issues/2738)) ([4474b34](https://github.com/ydb-platform/ydb-embedded-ui/commit/4474b34da09b575d2976b8b98839a4bc9f4f1907))
* rename PROMOTE to PROMOTED ([#2746](https://github.com/ydb-platform/ydb-embedded-ui/issues/2746)) ([d06c4de](https://github.com/ydb-platform/ydb-embedded-ui/commit/d06c4de7133cc81350559823f5a9e30469380577))

## [11.8.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v11.7.0...v11.8.0) (2025-08-20)


### Features

* mark UI-generated queries with internal_call=true parameter ([#2731](https://github.com/ydb-platform/ydb-embedded-ui/issues/2731)) ([62cc324](https://github.com/ydb-platform/ydb-embedded-ui/commit/62cc324fc7701a1b8423ae6aeacbd6b3d615792f))


### Bug Fixes

* import components styles in App scss ([#2737](https://github.com/ydb-platform/ydb-embedded-ui/issues/2737)) ([b2fd583](https://github.com/ydb-platform/ydb-embedded-ui/commit/b2fd5839f0cc005daae47141766270a298f40548))

## [11.7.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v11.6.0...v11.7.0) (2025-08-19)


### Features

* 2dc ([#2699](https://github.com/ydb-platform/ydb-embedded-ui/issues/2699)) ([a4b737b](https://github.com/ydb-platform/ydb-embedded-ui/commit/a4b737bc82927a2d47bba1c804931b8a66c0a885))
* add isBeingPromoted field to piles ([#2719](https://github.com/ydb-platform/ydb-embedded-ui/issues/2719)) ([75e92c3](https://github.com/ydb-platform/ydb-embedded-ui/commit/75e92c3112ec8e9eeaf71e11c02d47785c878f26))
* add metrics infrustructure to ui ([#2698](https://github.com/ydb-platform/ydb-embedded-ui/issues/2698)) ([e229c75](https://github.com/ydb-platform/ydb-embedded-ui/commit/e229c75bbb52a01355fdda38fec7e6c951cd793c))
* **Versions:** redesign ([#2707](https://github.com/ydb-platform/ydb-embedded-ui/issues/2707)) ([7a826b6](https://github.com/ydb-platform/ydb-embedded-ui/commit/7a826b6a3544aed63330cb9e47496c9c3473305c))


### Bug Fixes

* 2dc format from backend ([#2733](https://github.com/ydb-platform/ydb-embedded-ui/issues/2733)) ([dcded29](https://github.com/ydb-platform/ydb-embedded-ui/commit/dcded291bd0d7fd6ebed8a81b9cb6cfc95130407))
* broken tests for healthcheck ([#2705](https://github.com/ydb-platform/ydb-embedded-ui/issues/2705)) ([7734edb](https://github.com/ydb-platform/ydb-embedded-ui/commit/7734edb74f117a71598305b030e4a2b78bd03335))
* **Content:** do not request whoami if auth is in progress ([#2727](https://github.com/ydb-platform/ydb-embedded-ui/issues/2727)) ([a1ff7a2](https://github.com/ydb-platform/ydb-embedded-ui/commit/a1ff7a293ffac6c8597546a8ae45abbb725ee0a2))
* fixed the transfer status value ([#2726](https://github.com/ydb-platform/ydb-embedded-ui/issues/2726)) ([a049065](https://github.com/ydb-platform/ydb-embedded-ui/commit/a04906590b2f690caeee92859269f1c061c24825))
* **query:** incorrect selector ([#2703](https://github.com/ydb-platform/ydb-embedded-ui/issues/2703)) ([7e3aabb](https://github.com/ydb-platform/ydb-embedded-ui/commit/7e3aabb97bd027b8513a8cdecde830978fe8c105))
* **ResultSetsViewer:** recount rows if fullscreen ([#2704](https://github.com/ydb-platform/ydb-embedded-ui/issues/2704)) ([cc751a5](https://github.com/ydb-platform/ydb-embedded-ui/commit/cc751a591d3337bfc28db8f4cababafbef68f608))
* **SpeedMultiMeter:** update title it new props ([#2700](https://github.com/ydb-platform/ydb-embedded-ui/issues/2700)) ([69bdb59](https://github.com/ydb-platform/ydb-embedded-ui/commit/69bdb59705cdb4b496fb1fb85abd11d2f1e367e0))

## [11.6.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v11.5.0...v11.6.0) (2025-08-08)


### Features

* add new role IsDatabaseAllowed and shrink breadcrumbs ([#2672](https://github.com/ydb-platform/ydb-embedded-ui/issues/2672)) ([66b45ee](https://github.com/ydb-platform/ydb-embedded-ui/commit/66b45ee81ad4f4088746dc4fef878d249706f4f4))
* **Diagnostics:** hide AccessRights is not configured ([#2691](https://github.com/ydb-platform/ydb-embedded-ui/issues/2691)) ([76275c4](https://github.com/ydb-platform/ydb-embedded-ui/commit/76275c4cf0ecf2f893c14350f311a8d401f81f6c))
* **Header:** add manage DB dropdown ([#2680](https://github.com/ydb-platform/ydb-embedded-ui/issues/2680)) ([d123263](https://github.com/ydb-platform/ydb-embedded-ui/commit/d1232631a271fa1b26e953935707b46134b8534d))
* **HealthcheckPreview:** do not show preview for green status ([#2687](https://github.com/ydb-platform/ydb-embedded-ui/issues/2687)) ([09fd897](https://github.com/ydb-platform/ydb-embedded-ui/commit/09fd897bc9145abcbd8e11ce3c8672b673e31e10))
* improve guidelines basing on reviews ([#2662](https://github.com/ydb-platform/ydb-embedded-ui/issues/2662)) ([932917b](https://github.com/ydb-platform/ydb-embedded-ui/commit/932917b9d8ce0a5b8eb563280eaf59cd1c093b7e))
* **ObjectSummary:** remove Acl ([#2664](https://github.com/ydb-platform/ydb-embedded-ui/issues/2664)) ([a49edb0](https://github.com/ydb-platform/ydb-embedded-ui/commit/a49edb0ba501d505580f69af84ccf9a1bcb3bbe1))
* restrictions for not IsViewerUser ([#2684](https://github.com/ydb-platform/ydb-embedded-ui/issues/2684)) ([a458c8f](https://github.com/ydb-platform/ydb-embedded-ui/commit/a458c8f802646b38b75a5f6742d2d00ddd1a1d5e))
* restrictions for users without IsViewerAllowed role ([#2675](https://github.com/ydb-platform/ydb-embedded-ui/issues/2675)) ([fae206e](https://github.com/ydb-platform/ydb-embedded-ui/commit/fae206ef240adfbd302a5e0fc76aa9b34ace7d40))


### Bug Fixes

* conditionally show threads tab based on API response data ([#2666](https://github.com/ydb-platform/ydb-embedded-ui/issues/2666)) ([d73717a](https://github.com/ydb-platform/ydb-embedded-ui/commit/d73717a0d28663aa63ca5cf4eaaef81505674962))
* do not remove build from versions ([#2688](https://github.com/ydb-platform/ydb-embedded-ui/issues/2688)) ([91c3844](https://github.com/ydb-platform/ydb-embedded-ui/commit/91c38446d2fd1ae47515dea34691b9fac008d2da))
* fixed transfer page ([#2683](https://github.com/ydb-platform/ydb-embedded-ui/issues/2683)) ([0c05cc3](https://github.com/ydb-platform/ydb-embedded-ui/commit/0c05cc3081e947f4cad2eb41309068e4b9921ce4))
* **PaginatedTable:** no bottom border for last row ([#2676](https://github.com/ydb-platform/ydb-embedded-ui/issues/2676)) ([4ad88f9](https://github.com/ydb-platform/ydb-embedded-ui/commit/4ad88f9c5a9ec5c7dce11980ab7ae87ea432c0bc))
* **Tenant:** remove falsy query params from address ([#2690](https://github.com/ydb-platform/ydb-embedded-ui/issues/2690)) ([633b184](https://github.com/ydb-platform/ydb-embedded-ui/commit/633b18472e28f73eea73b778aac36cff527feeb4))

## [11.5.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v11.4.0...v11.5.0) (2025-08-01)


### Features

* add new tab to Node page with thread pool statistics ([#2599](https://github.com/ydb-platform/ydb-embedded-ui/issues/2599)) ([c945a78](https://github.com/ydb-platform/ydb-embedded-ui/commit/c945a78642c28b88c470f735b4e21e0b6fefb91b))
* **api:** use custom axios defaults ([#2616](https://github.com/ydb-platform/ydb-embedded-ui/issues/2616)) ([7397f5f](https://github.com/ydb-platform/ydb-embedded-ui/commit/7397f5f7a0715ed1faed149da9d09c3441b482d0))
* **Clusters:** rework versions progress bar ([#2642](https://github.com/ydb-platform/ydb-embedded-ui/issues/2642)) ([7be9d5f](https://github.com/ydb-platform/ydb-embedded-ui/commit/7be9d5f559745804cacf5e02b5c844648e191618))
* implement reach/impact/effort priority methodology ([#2615](https://github.com/ydb-platform/ydb-embedded-ui/issues/2615)) ([6adb6d7](https://github.com/ydb-platform/ydb-embedded-ui/commit/6adb6d7b6f7c1cdd408684ae4729abc93d264460))
* improve tenant diagnostics - CPU ([#2633](https://github.com/ydb-platform/ydb-embedded-ui/issues/2633)) ([84e75ea](https://github.com/ydb-platform/ydb-embedded-ui/commit/84e75eac53b7413e1087d027c9a70854dcff00e3))
* keep plan representation tab selection between query executions ([#2625](https://github.com/ydb-platform/ydb-embedded-ui/issues/2625)) ([151d766](https://github.com/ydb-platform/ydb-embedded-ui/commit/151d7663e8d1416c35269c9f13fd09a994b60c89))
* redesign CPU section ([#2597](https://github.com/ydb-platform/ydb-embedded-ui/issues/2597)) ([c170578](https://github.com/ydb-platform/ydb-embedded-ui/commit/c17057823931915356efc4c7a3ff9d48f1381206))
* redesign Memory section ([#2627](https://github.com/ydb-platform/ydb-embedded-ui/issues/2627)) ([437a81c](https://github.com/ydb-platform/ydb-embedded-ui/commit/437a81ce3b95a3d320ce951e7a5328aac407c28e))
* redesign Storage section ([#2608](https://github.com/ydb-platform/ydb-embedded-ui/issues/2608)) ([4222c14](https://github.com/ydb-platform/ydb-embedded-ui/commit/4222c1412f67a90a69d88c315c119fed0768b662))
* **TenantOverview:** do not use tabs ([#2655](https://github.com/ydb-platform/ydb-embedded-ui/issues/2655)) ([4fb69ad](https://github.com/ydb-platform/ydb-embedded-ui/commit/4fb69adcc61a64ed7becd663ca8b5781f25e37e4))


### Bug Fixes

* add network section ([#2635](https://github.com/ydb-platform/ydb-embedded-ui/issues/2635)) ([b88c30e](https://github.com/ydb-platform/ydb-embedded-ui/commit/b88c30edb8e5a8175a39e6fbd5135e903e8469b7))
* **api:** constructor types ([#2613](https://github.com/ydb-platform/ydb-embedded-ui/issues/2613)) ([d2bdf9f](https://github.com/ydb-platform/ydb-embedded-ui/commit/d2bdf9f2ddb134424e7bf5a18db48db76472bd5a))
* **AsideNavigation:** hotkeys section position ([#2629](https://github.com/ydb-platform/ydb-embedded-ui/issues/2629)) ([87aca43](https://github.com/ydb-platform/ydb-embedded-ui/commit/87aca43ee8acd8e871886b7ebd6cd88abe1ad0f6))
* **Clusters:** fix count single form ([#2636](https://github.com/ydb-platform/ydb-embedded-ui/issues/2636)) ([b789b1b](https://github.com/ydb-platform/ydb-embedded-ui/commit/b789b1bdcd4f25234de875be09c2b8a4e748dcdb))
* code review workflows to checkout PR head commit ([#2652](https://github.com/ydb-platform/ydb-embedded-ui/issues/2652)) ([c6b9fe5](https://github.com/ydb-platform/ydb-embedded-ui/commit/c6b9fe581bbcc132ed5b9a81c771a8406488326a))
* design refinements of query banner ([#2630](https://github.com/ydb-platform/ydb-embedded-ui/issues/2630)) ([30c6427](https://github.com/ydb-platform/ydb-embedded-ui/commit/30c6427924b408b76a7f1b19674ca1d26ebe963f))
* incorrect storage group count ([#2653](https://github.com/ydb-platform/ydb-embedded-ui/issues/2653)) ([15a93fd](https://github.com/ydb-platform/ydb-embedded-ui/commit/15a93fd20ba0a06d2e7fb4ea23315bb0f164c739))
* jumping content in database tabs ([#2606](https://github.com/ydb-platform/ydb-embedded-ui/issues/2606)) ([602ca15](https://github.com/ydb-platform/ydb-embedded-ui/commit/602ca151509eaa0bfafe29777c12d1aa917da07a))
* **NetworkTable:** fix columns align ([#2640](https://github.com/ydb-platform/ydb-embedded-ui/issues/2640)) ([03d315a](https://github.com/ydb-platform/ydb-embedded-ui/commit/03d315a86cd9444fb7951dd936ab6038f42671a9))
* remove keybindings for history traversing ([#2611](https://github.com/ydb-platform/ydb-embedded-ui/issues/2611)) ([18af440](https://github.com/ydb-platform/ydb-embedded-ui/commit/18af4401d10403f4bcf4061ba3ac7c8ebfd56379))
* tenant navigation ([#2649](https://github.com/ydb-platform/ydb-embedded-ui/issues/2649)) ([5918555](https://github.com/ydb-platform/ydb-embedded-ui/commit/59185556918e0492fa42841ae99fcdd9c6981893))
* **TenantNetwork:** use correct settings for tables ([#2639](https://github.com/ydb-platform/ydb-embedded-ui/issues/2639)) ([93e8c52](https://github.com/ydb-platform/ydb-embedded-ui/commit/93e8c52426d0e12e71b63f0f5d9b5dc06caa93e3))
* **VersionsBar:** add debounce to mouse leave ([#2656](https://github.com/ydb-platform/ydb-embedded-ui/issues/2656)) ([812865a](https://github.com/ydb-platform/ydb-embedded-ui/commit/812865add708c1f9a6a4d7183d9199881bfa9d00))

## [11.4.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v11.3.0...v11.4.0) (2025-07-23)


### Features

* add support for csrf ([#2604](https://github.com/ydb-platform/ydb-embedded-ui/issues/2604)) ([7a4505f](https://github.com/ydb-platform/ydb-embedded-ui/commit/7a4505f8b79d17e2791f98dd2b881dcf03b0243c))
* **prepareErrorMessage:** treat error.data as object ([#2601](https://github.com/ydb-platform/ydb-embedded-ui/issues/2601)) ([4937cd3](https://github.com/ydb-platform/ydb-embedded-ui/commit/4937cd37170257351d5618525c14cb7fb52a3562))
* update versions colors ([#2596](https://github.com/ydb-platform/ydb-embedded-ui/issues/2596)) ([17c8d29](https://github.com/ydb-platform/ydb-embedded-ui/commit/17c8d292a709d9ca9817f65bf0ab6e1dfded424e))


### Bug Fixes

* use svg instead of mask for doughnuts ([#2600](https://github.com/ydb-platform/ydb-embedded-ui/issues/2600)) ([dcbefa1](https://github.com/ydb-platform/ydb-embedded-ui/commit/dcbefa1fba1445323d265f7a3105f29416693fa8))

## [11.3.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v11.2.0...v11.3.0) (2025-07-23)


### Features

* rework metric cards to tabs ([#2580](https://github.com/ydb-platform/ydb-embedded-ui/issues/2580)) ([016a515](https://github.com/ydb-platform/ydb-embedded-ui/commit/016a5158cec7cb32e69a4d76cf35c0a0e6f122a5))
* use custom app title ([#2584](https://github.com/ydb-platform/ydb-embedded-ui/issues/2584)) ([c09d9be](https://github.com/ydb-platform/ydb-embedded-ui/commit/c09d9be6cabf835b3a20e6e47ff4909ce7502739))
* **VDisk:** add replication progress, remaining time, donors info ([#2588](https://github.com/ydb-platform/ydb-embedded-ui/issues/2588)) ([f50686c](https://github.com/ydb-platform/ydb-embedded-ui/commit/f50686c9f906512406545d2df6ed3f545567e54c))


### Bug Fixes

* **SpeedMuliMeter:** add popup content padding ([#2585](https://github.com/ydb-platform/ydb-embedded-ui/issues/2585)) ([dd0108a](https://github.com/ydb-platform/ydb-embedded-ui/commit/dd0108a7db8b2aae6532ea0d7233d5537a7c9499))

## [11.2.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v11.1.1...v11.2.0) (2025-07-18)


### Features

* **configureStore:** use custom backend ([#2583](https://github.com/ydb-platform/ydb-embedded-ui/issues/2583)) ([1a2aa24](https://github.com/ydb-platform/ydb-embedded-ui/commit/1a2aa242f312b7a39d7865f243df72518660d65e))
* **query:** apply pragmas setting to Table Preview queries ([#2575](https://github.com/ydb-platform/ydb-embedded-ui/issues/2575)) ([ff85ffc](https://github.com/ydb-platform/ydb-embedded-ui/commit/ff85ffc907716dcde07725fb3d4781834fe012ef))
* **tablet:** improve tablet page layout: swap places type and title ([#2581](https://github.com/ydb-platform/ydb-embedded-ui/issues/2581)) ([ac9bc21](https://github.com/ydb-platform/ydb-embedded-ui/commit/ac9bc21f08a8920f6f0b2102bff1043e5484de21))
* **vdisk:** add tablet usage statistics to vdisk page ([#2577](https://github.com/ydb-platform/ydb-embedded-ui/issues/2577)) ([4ca86da](https://github.com/ydb-platform/ydb-embedded-ui/commit/4ca86da7f63027830471d4e9cc50b8274d81be12))

## [11.1.1](https://github.com/ydb-platform/ydb-embedded-ui/compare/v11.1.0...v11.1.1) (2025-07-15)


### Bug Fixes

* query activities banner visibility ([#2572](https://github.com/ydb-platform/ydb-embedded-ui/issues/2572)) ([c021e65](https://github.com/ydb-platform/ydb-embedded-ui/commit/c021e651a4053b32fe24572919b28d06b5beac6d))
* **StorageLocation:** pdisk id calculation ([#2570](https://github.com/ydb-platform/ydb-embedded-ui/issues/2570)) ([8c9b492](https://github.com/ydb-platform/ydb-embedded-ui/commit/8c9b492bfaef377d7b7545187b7bccae2a065db6))

## [11.1.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v11.0.1...v11.1.0) (2025-07-15)


### Features

* add Queries activities banner ([#2549](https://github.com/ydb-platform/ydb-embedded-ui/issues/2549)) ([dcfca43](https://github.com/ydb-platform/ydb-embedded-ui/commit/dcfca43edffca6b5e8ee568e0ea7ff4bb8547654))
* **query:** add multi-line Pragmas field to Query Settings Dialog ([#2563](https://github.com/ydb-platform/ydb-embedded-ui/issues/2563)) ([2010164](https://github.com/ydb-platform/ydb-embedded-ui/commit/2010164e2ee680bcfe31b7d83b35deae2bb09bb8))


### Bug Fixes

* cancel previous multipart request if the new one starts ([#2559](https://github.com/ydb-platform/ydb-embedded-ui/issues/2559)) ([a4792fd](https://github.com/ydb-platform/ydb-embedded-ui/commit/a4792fd90edfc8e84d1ff3f31b380c417e25175e))
* disable streaming by default for some clusters ([#2566](https://github.com/ydb-platform/ydb-embedded-ui/issues/2566)) ([a9670eb](https://github.com/ydb-platform/ydb-embedded-ui/commit/a9670eb128ae69b2c61bf0794defd3d40de55eea))
* **TopicData:** reset filters on partition change ([#2567](https://github.com/ydb-platform/ydb-embedded-ui/issues/2567)) ([848f754](https://github.com/ydb-platform/ydb-embedded-ui/commit/848f754f3d9964c9218ea670bd76276ddb37b82a))

## [11.0.1](https://github.com/ydb-platform/ydb-embedded-ui/compare/v11.0.0...v11.0.1) (2025-07-09)


### Bug Fixes

* **TopicData:** decode data for message, show error for entire table ([#2554](https://github.com/ydb-platform/ydb-embedded-ui/issues/2554)) ([b56cd82](https://github.com/ydb-platform/ydb-embedded-ui/commit/b56cd827b91d2ce3c42d3cec3a8877267f1a7092))

## [11.0.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v10.1.1...v11.0.0) (2025-07-09)


### ⚠ BREAKING CHANGES

* update to uikit7 ([#2544](https://github.com/ydb-platform/ydb-embedded-ui/issues/2544))

### Features

* update to uikit7 ([#2544](https://github.com/ydb-platform/ydb-embedded-ui/issues/2544)) ([9716a69](https://github.com/ydb-platform/ydb-embedded-ui/commit/9716a695627eff4ad888e4641086fafcff5e3e6d))

## [10.1.1](https://github.com/ydb-platform/ydb-embedded-ui/compare/v10.1.0...v10.1.1) (2025-07-08)


### Bug Fixes

* **capabilities:** show topic data from 1 handler version ([#2529](https://github.com/ydb-platform/ydb-embedded-ui/issues/2529)) ([de6b802](https://github.com/ydb-platform/ydb-embedded-ui/commit/de6b8024a28fc362fcdfc3faf98d0eef802e46b7))

## [10.1.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v10.0.0...v10.1.0) (2025-07-07)


### Features

* update versions colors ([#2530](https://github.com/ydb-platform/ydb-embedded-ui/issues/2530)) ([a392546](https://github.com/ydb-platform/ydb-embedded-ui/commit/a39254666dff7b323e7d499ee301ceeddf46e8d0))


### Bug Fixes

* enable oidc streaming ([#2537](https://github.com/ydb-platform/ydb-embedded-ui/issues/2537)) ([fa98022](https://github.com/ydb-platform/ydb-embedded-ui/commit/fa98022bc25fc1a8636014b958552f9f0b36f0b2))
* remove operations counter ([#2540](https://github.com/ydb-platform/ydb-embedded-ui/issues/2540)) ([4821f40](https://github.com/ydb-platform/ydb-embedded-ui/commit/4821f40da2e710d3b5345b269dd04aca5855e8ea))

## [10.0.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v9.11.0...v10.0.0) (2025-07-03)


### ⚠ BREAKING CHANGES

* remove ai assistant button ([#2535](https://github.com/ydb-platform/ydb-embedded-ui/issues/2535))

### Features

* remove ai assistant button ([#2535](https://github.com/ydb-platform/ydb-embedded-ui/issues/2535)) ([2511059](https://github.com/ydb-platform/ydb-embedded-ui/commit/2511059fc05ef035a4b44f71434f20f462b96c21))


### Bug Fixes

* make operations page_size to 20 ([#2498](https://github.com/ydb-platform/ydb-embedded-ui/issues/2498)) ([512cf15](https://github.com/ydb-platform/ydb-embedded-ui/commit/512cf159374b0fa416910540ed437ef4fa840946))
* remove viewer from balancer ([#2523](https://github.com/ydb-platform/ydb-embedded-ui/issues/2523)) ([c37f2e3](https://github.com/ydb-platform/ydb-embedded-ui/commit/c37f2e3609d97ba35caa9010d8b8a8be1881a661))

## [9.11.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v9.10.0...v9.11.0) (2025-06-30)


### Features

* **Clusters:** redesign table ([#2495](https://github.com/ydb-platform/ydb-embedded-ui/issues/2495)) ([7fa0358](https://github.com/ydb-platform/ydb-embedded-ui/commit/7fa0358d0be8572cb0bb46d6ac7b59de5f58e262))


### Bug Fixes

* **HealthcheckPreview:** enable autorefresh for all clusters ([#2512](https://github.com/ydb-platform/ydb-embedded-ui/issues/2512)) ([6ed077e](https://github.com/ydb-platform/ydb-embedded-ui/commit/6ed077ef1bb5a78774e43f875d3d37cee0f61f43))
* unskip tests ([#2514](https://github.com/ydb-platform/ydb-embedded-ui/issues/2514)) ([0198726](https://github.com/ydb-platform/ydb-embedded-ui/commit/0198726db84917950799ae757479e50acd90948b))
* **VDiskPage:** display params in 2 columns, change order ([#2479](https://github.com/ydb-platform/ydb-embedded-ui/issues/2479)) ([2e0f203](https://github.com/ydb-platform/ydb-embedded-ui/commit/2e0f2035c8bc61406f746fa15b697a1fde646fa7))

## [9.10.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v9.9.1...v9.10.0) (2025-06-30)


### Features

* **Tenants:** add network column ([#2478](https://github.com/ydb-platform/ydb-embedded-ui/issues/2478)) ([f3b573a](https://github.com/ydb-platform/ydb-embedded-ui/commit/f3b573aad122b786e3c3baf655ebc626b3d3fe05))


### Bug Fixes

* aibutton placeholder header ([#2499](https://github.com/ydb-platform/ydb-embedded-ui/issues/2499)) ([f2c967b](https://github.com/ydb-platform/ydb-embedded-ui/commit/f2c967bb1efc74f2c961463dbf7e750236d32008))
* interface blinks on reload with dark theme ([#2480](https://github.com/ydb-platform/ydb-embedded-ui/issues/2480)) ([d6f23a3](https://github.com/ydb-platform/ydb-embedded-ui/commit/d6f23a31380fea63368ce986f643e3c5880fd7d7))
* **JsonViewer:** do not try to decode utf8 ([#2509](https://github.com/ydb-platform/ydb-embedded-ui/issues/2509)) ([141c7e0](https://github.com/ydb-platform/ydb-embedded-ui/commit/141c7e0b52882f51adff4d35c51a72d4ea8f8cbb))
* **Network:** host in default and required columns ([#2511](https://github.com/ydb-platform/ydb-embedded-ui/issues/2511)) ([1affd72](https://github.com/ydb-platform/ydb-embedded-ui/commit/1affd723d2f9a6912869c92aa6caa7315a7f0514))
* require ConnectStatus only for network table ([#2510](https://github.com/ydb-platform/ydb-embedded-ui/issues/2510)) ([d9a049f](https://github.com/ydb-platform/ydb-embedded-ui/commit/d9a049fbe9af38652e4a529550322e8bb60593d6))

## [9.9.1](https://github.com/ydb-platform/ydb-embedded-ui/compare/v9.9.0...v9.9.1) (2025-06-26)


### Bug Fixes

* code assist ([#2491](https://github.com/ydb-platform/ydb-embedded-ui/issues/2491)) ([46e0c9e](https://github.com/ydb-platform/ydb-embedded-ui/commit/46e0c9ee5161b546f01c4ec4982cea9df3709f4f))
* network error and incorrect backend response for operations ([#2489](https://github.com/ydb-platform/ydb-embedded-ui/issues/2489)) ([df6342b](https://github.com/ydb-platform/ydb-embedded-ui/commit/df6342bd3046193e51f89d195a9be1fc67a591bb))

## [9.9.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v9.8.0...v9.9.0) (2025-06-26)


### Features

* ai assistant placeholders ([#2483](https://github.com/ydb-platform/ydb-embedded-ui/issues/2483)) ([7675e82](https://github.com/ydb-platform/ydb-embedded-ui/commit/7675e82d3a73724bf888aa7968a9cf164fae1a24))


### Bug Fixes

* access denied for operations ([#2485](https://github.com/ydb-platform/ydb-embedded-ui/issues/2485)) ([12a3bcf](https://github.com/ydb-platform/ydb-embedded-ui/commit/12a3bcf2189d18c6ab0b893601438bb34f3fc2b6))
* do not show backups if no control_plane ([#2487](https://github.com/ydb-platform/ydb-embedded-ui/issues/2487)) ([d6f29a2](https://github.com/ydb-platform/ydb-embedded-ui/commit/d6f29a216fc696fa19acd6c32b602d7ab1d779c0))

## [9.8.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v9.7.1...v9.8.0) (2025-06-25)


### Features

* add setting to select syntax in acl handlers ([#2474](https://github.com/ydb-platform/ydb-embedded-ui/issues/2474)) ([e5e6f3a](https://github.com/ydb-platform/ydb-embedded-ui/commit/e5e6f3ad87ac331ec99011571701b6a878d9f3d7))
* **api:** add new possible tag ([#2476](https://github.com/ydb-platform/ydb-embedded-ui/issues/2476)) ([ec81875](https://github.com/ydb-platform/ydb-embedded-ui/commit/ec81875a9c1dcee7c5cfce34908da52f280f6a86))
* **Cluster:** add Network tab ([#2424](https://github.com/ydb-platform/ydb-embedded-ui/issues/2424)) ([7af71c4](https://github.com/ydb-platform/ydb-embedded-ui/commit/7af71c478775ef70090e21fe60ac07ebc21534b6))


### Bug Fixes

* code-assistant requests 404 ([#2471](https://github.com/ydb-platform/ydb-embedded-ui/issues/2471)) ([1eb4e01](https://github.com/ydb-platform/ydb-embedded-ui/commit/1eb4e01b7a8eedf30bb3a2b47afa987788c368e7))
* operations tab ([#2435](https://github.com/ydb-platform/ydb-embedded-ui/issues/2435)) ([937c561](https://github.com/ydb-platform/ydb-embedded-ui/commit/937c561c617bf4cf73c472631623ce6bf40db59f))
* remove host fqdn and node id columns from node tablets table ([#2473](https://github.com/ydb-platform/ydb-embedded-ui/issues/2473)) ([6e598fd](https://github.com/ydb-platform/ydb-embedded-ui/commit/6e598fdb6d37ada84ef1e4cbb6c8c3d1045ed59c))

## [9.7.1](https://github.com/ydb-platform/ydb-embedded-ui/compare/v9.7.0...v9.7.1) (2025-06-23)


### Bug Fixes

* **AccessRights:** no need in forms ([#2457](https://github.com/ydb-platform/ydb-embedded-ui/issues/2457)) ([03f238e](https://github.com/ydb-platform/ydb-embedded-ui/commit/03f238e5af4298bb23bebb9221c60b33b311b4a9))
* **Clusters:** filter empty DC ([#2453](https://github.com/ydb-platform/ydb-embedded-ui/issues/2453)) ([a106b5e](https://github.com/ydb-platform/ydb-embedded-ui/commit/a106b5e5aeb066b8f84e30d87c9c9b9af7cfd2f0))
* **Clusters:** size s for actions menu ([#2451](https://github.com/ydb-platform/ydb-embedded-ui/issues/2451)) ([d9a4c44](https://github.com/ydb-platform/ydb-embedded-ui/commit/d9a4c441dd3f4a62da1104d98f2c50ac3df094ac))
* **Cluster:** use cluster name if title is empty string ([#2452](https://github.com/ydb-platform/ydb-embedded-ui/issues/2452)) ([1c8a6c0](https://github.com/ydb-platform/ydb-embedded-ui/commit/1c8a6c0eb52d93f7becaff718b4590d346804d9f))
* disable oidc streaming ([#2462](https://github.com/ydb-platform/ydb-embedded-ui/issues/2462)) ([3bffcbd](https://github.com/ydb-platform/ydb-embedded-ui/commit/3bffcbd1f9ff61500fb6c05c2ac3c371a0bc53a5))
* do not expose obsolet name param ([#2458](https://github.com/ydb-platform/ydb-embedded-ui/issues/2458)) ([736c5f0](https://github.com/ydb-platform/ydb-embedded-ui/commit/736c5f0478eca2f38b3dc513278aae6a358dcefe))

## [9.7.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v9.6.3...v9.7.0) (2025-06-20)


### Features

* add backups page with custom render ([#2442](https://github.com/ydb-platform/ydb-embedded-ui/issues/2442)) ([b66e8d5](https://github.com/ydb-platform/ydb-embedded-ui/commit/b66e8d50bdf23044f103850310bdec55a400ee54))
* manage schema object permissions ([#2398](https://github.com/ydb-platform/ydb-embedded-ui/issues/2398)) ([0c89baa](https://github.com/ydb-platform/ydb-embedded-ui/commit/0c89baa3d9f33ae8cc9b67a101815bdcbf5a890d))


### Bug Fixes

* **HealthcheckPreview:** use only one query ([#2427](https://github.com/ydb-platform/ydb-embedded-ui/issues/2427)) ([74ca103](https://github.com/ydb-platform/ydb-embedded-ui/commit/74ca10362e45a062fccdd8b4a7691abb7bcc8a4d))
* **healthcheck:** show groups ids in separate rows ([#2423](https://github.com/ydb-platform/ydb-embedded-ui/issues/2423)) ([eaa789b](https://github.com/ydb-platform/ydb-embedded-ui/commit/eaa789bcdb320ebec137aace2c1e055ef999bd2c))
* **Network:** require ConnectStatus for host column ([#2429](https://github.com/ydb-platform/ydb-embedded-ui/issues/2429)) ([eccba69](https://github.com/ydb-platform/ydb-embedded-ui/commit/eccba6912daecf540d435b3df2eb3e186ab815ff))
* **Nodes:** request tablets only if required ([#2433](https://github.com/ydb-platform/ydb-embedded-ui/issues/2433)) ([e1499e9](https://github.com/ydb-platform/ydb-embedded-ui/commit/e1499e9912692e3714708e574f87c77c3d507940))
* support name param for database ([#2426](https://github.com/ydb-platform/ydb-embedded-ui/issues/2426)) ([ce1fb66](https://github.com/ydb-platform/ydb-embedded-ui/commit/ce1fb6639030e608bab6628f4c45cfd10e9512a5))
* tests ([#2439](https://github.com/ydb-platform/ydb-embedded-ui/issues/2439)) ([6001e22](https://github.com/ydb-platform/ydb-embedded-ui/commit/6001e22d0b24abf1ff07b8548f9208c494b0afe7))
* unskip tests ([#2436](https://github.com/ydb-platform/ydb-embedded-ui/issues/2436)) ([76b37bb](https://github.com/ydb-platform/ydb-embedded-ui/commit/76b37bbe8ec36bb91481fbf636bd6d038c5eb726))

## [9.6.3](https://github.com/ydb-platform/ydb-embedded-ui/compare/v9.6.2...v9.6.3) (2025-06-17)


### Bug Fixes

* check stats independently of plan ([#2413](https://github.com/ydb-platform/ydb-embedded-ui/issues/2413)) ([3b554c8](https://github.com/ydb-platform/ydb-embedded-ui/commit/3b554c867345dcd13909484785b71cbf1f215472))

## [9.6.2](https://github.com/ydb-platform/ydb-embedded-ui/compare/v9.6.1...v9.6.2) (2025-06-17)


### Bug Fixes

* **healthcheck:** all fields may be undefined ([#2410](https://github.com/ydb-platform/ydb-embedded-ui/issues/2410)) ([6d2f450](https://github.com/ydb-platform/ydb-embedded-ui/commit/6d2f45002b8ca5be7d323a727fd01c7e59ada221))
* **Healthcheck:** do not autorefresh ([#2409](https://github.com/ydb-platform/ydb-embedded-ui/issues/2409)) ([7876ecb](https://github.com/ydb-platform/ydb-embedded-ui/commit/7876ecb7c98a443874e3c564d16c9a27855daaf5))

## [9.6.1](https://github.com/ydb-platform/ydb-embedded-ui/compare/v9.6.0...v9.6.1) (2025-06-16)


### Bug Fixes

* **uiFactory:** add generic support ([#2407](https://github.com/ydb-platform/ydb-embedded-ui/issues/2407)) ([0014557](https://github.com/ydb-platform/ydb-embedded-ui/commit/00145578ae6d548763d66af2fe8e78c1e37dac5c))

## [9.6.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v9.5.1...v9.6.0) (2025-06-16)


### Features

* evict unhealthy VDisk ([#2399](https://github.com/ydb-platform/ydb-embedded-ui/issues/2399)) ([7594f70](https://github.com/ydb-platform/ydb-embedded-ui/commit/7594f70684da836d9bcb9dc71c14507b0d1fa947))


### Bug Fixes

* **Healthcheck:** types ([#2402](https://github.com/ydb-platform/ydb-embedded-ui/issues/2402)) ([e09cc13](https://github.com/ydb-platform/ydb-embedded-ui/commit/e09cc1319025076f93b4169ba2e77d5b4d920e0d))

## [9.5.1](https://github.com/ydb-platform/ydb-embedded-ui/compare/v9.5.0...v9.5.1) (2025-06-11)


### Bug Fixes

* **Clusters:** increase menu items size ([#2396](https://github.com/ydb-platform/ydb-embedded-ui/issues/2396)) ([946efb3](https://github.com/ydb-platform/ydb-embedded-ui/commit/946efb3d60a8bc0da52dc8dbe7bf82641073dcf1))

## [9.5.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v9.4.1...v9.5.0) (2025-06-11)


### Features

* allow defining a list of links for cluster and database overviews ([#2359](https://github.com/ydb-platform/ydb-embedded-ui/issues/2359)) ([3aef421](https://github.com/ydb-platform/ydb-embedded-ui/commit/3aef4212fff6a9cbf672b02757e6abc83b1bf079))


### Bug Fixes

* **Clusters:** fix scroll ([#2394](https://github.com/ydb-platform/ydb-embedded-ui/issues/2394)) ([9ba0f4a](https://github.com/ydb-platform/ydb-embedded-ui/commit/9ba0f4aee28d6446baa67065814cbc36192d6f92))
* **Clusters:** make actions switcher secondary ([#2393](https://github.com/ydb-platform/ydb-embedded-ui/issues/2393)) ([cc3d8e3](https://github.com/ydb-platform/ydb-embedded-ui/commit/cc3d8e3ce72def0975a9f72c3238706c4676d861))

## [9.4.1](https://github.com/ydb-platform/ydb-embedded-ui/compare/v9.4.0...v9.4.1) (2025-06-10)


### Bug Fixes

* **uiFactory:** make overrides partial ([#2384](https://github.com/ydb-platform/ydb-embedded-ui/issues/2384)) ([a54999e](https://github.com/ydb-platform/ydb-embedded-ui/commit/a54999ee3e52b73d7764e5d91faf412ed750be33))

## [9.4.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v9.3.1...v9.4.0) (2025-06-09)


### Features

* **Clusters:** allow add, edit and delete cluster funcs ([#2369](https://github.com/ydb-platform/ydb-embedded-ui/issues/2369)) ([69b4b4e](https://github.com/ydb-platform/ydb-embedded-ui/commit/69b4b4e80453f1687154f5a7e0c68d6fb70bdfc9))
* **createToast:** allow more props from uikit ([#2370](https://github.com/ydb-platform/ydb-embedded-ui/issues/2370)) ([0dda6ec](https://github.com/ydb-platform/ydb-embedded-ui/commit/0dda6ec77eb003ef2785f86e62cc69b8c6ec7644))
* drawer table scroll ([#2364](https://github.com/ydb-platform/ydb-embedded-ui/issues/2364)) ([34ea140](https://github.com/ydb-platform/ydb-embedded-ui/commit/34ea140e58091f4065f2f7b23b07de12d9dd1f3c))
* **Healthcheck:** redesign ([#2348](https://github.com/ydb-platform/ydb-embedded-ui/issues/2348)) ([eac76af](https://github.com/ydb-platform/ydb-embedded-ui/commit/eac76afd3a3dca4d80a73b56af4e859146603d27))


### Bug Fixes

* **TopicPreview:** dont show negative offsets ([#2381](https://github.com/ydb-platform/ydb-embedded-ui/issues/2381)) ([91deb63](https://github.com/ydb-platform/ydb-embedded-ui/commit/91deb634669fa48fa8ea9a670d015ae769aefceb))

## [9.3.1](https://github.com/ydb-platform/ydb-embedded-ui/compare/v9.3.0...v9.3.1) (2025-06-03)


### Bug Fixes

* credentials for multipart for cross-origin ([#2361](https://github.com/ydb-platform/ydb-embedded-ui/issues/2361)) ([97dfc75](https://github.com/ydb-platform/ydb-embedded-ui/commit/97dfc750ef8a086303caba6452511e3d996215ab))
* drawer is broken ([#2351](https://github.com/ydb-platform/ydb-embedded-ui/issues/2351)) ([ce13b7f](https://github.com/ydb-platform/ydb-embedded-ui/commit/ce13b7fca36d1e77f31221156ce09d1b6f35c418))
* scrolling performance optimizations for table ([#2335](https://github.com/ydb-platform/ydb-embedded-ui/issues/2335)) ([6c72cce](https://github.com/ydb-platform/ydb-embedded-ui/commit/6c72ccedc206bc3b18f8d20f41d7be3a60f64829))
* table is broken because of batching ([#2356](https://github.com/ydb-platform/ydb-embedded-ui/issues/2356)) ([7f3ea8c](https://github.com/ydb-platform/ydb-embedded-ui/commit/7f3ea8ca82581de6f2c10e473704cc30cf9697a7))
* table is not scrolled to top on sorting ([#2347](https://github.com/ydb-platform/ydb-embedded-ui/issues/2347)) ([7bde143](https://github.com/ydb-platform/ydb-embedded-ui/commit/7bde143c44535cc78f87b366e72f3213d584e0a9))
* **Tenants:** do not show actions for domain ([#2336](https://github.com/ydb-platform/ydb-embedded-ui/issues/2336)) ([828724d](https://github.com/ydb-platform/ydb-embedded-ui/commit/828724daf3ab8f0aa7ab6b6fd86c837e99be59d5))

## [9.3.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v9.2.2...v9.3.0) (2025-05-27)


### Features

* **Tenants:** pass full db data to onDelete ([#2328](https://github.com/ydb-platform/ydb-embedded-ui/issues/2328)) ([b966aaa](https://github.com/ydb-platform/ydb-embedded-ui/commit/b966aaaa8abeb55cea2f579da0f99dd5f458ef0b))


### Bug Fixes

* **Versions:** properly include nodes with defined roles in other group ([#2327](https://github.com/ydb-platform/ydb-embedded-ui/issues/2327)) ([3f3a543](https://github.com/ydb-platform/ydb-embedded-ui/commit/3f3a543a688fe2370053d95aa0c051da625055c9))

## [9.2.2](https://github.com/ydb-platform/ydb-embedded-ui/compare/v9.2.1...v9.2.2) (2025-05-26)


### Bug Fixes

* incorrect group numbers ([#2319](https://github.com/ydb-platform/ydb-embedded-ui/issues/2319)) ([a2e9cce](https://github.com/ydb-platform/ydb-embedded-ui/commit/a2e9cce5aed6840fc7e926dff0ffc8ac0a02d434))
* initial params in useEffect ([#2321](https://github.com/ydb-platform/ydb-embedded-ui/issues/2321)) ([28a51ed](https://github.com/ydb-platform/ydb-embedded-ui/commit/28a51ed39ff4d15fba01376b89c2dec95f573e79))
* update acl tests ([#2324](https://github.com/ydb-platform/ydb-embedded-ui/issues/2324)) ([84f0eee](https://github.com/ydb-platform/ydb-embedded-ui/commit/84f0eeeed3fb8490437cf9be8aa50476246afd33))

## [9.2.1](https://github.com/ydb-platform/ydb-embedded-ui/compare/v9.2.0...v9.2.1) (2025-05-22)


### Bug Fixes

* paginated table in groups scrolled to top on refresh ([#2291](https://github.com/ydb-platform/ydb-embedded-ui/issues/2291)) ([f4971ee](https://github.com/ydb-platform/ydb-embedded-ui/commit/f4971eeb4846ba9468094fe039d4c4acb5ceb74d))
* reset base offsets on selected partition change ([#2314](https://github.com/ydb-platform/ydb-embedded-ui/issues/2314)) ([6119f14](https://github.com/ydb-platform/ydb-embedded-ui/commit/6119f1490a0bb4c5f3e21ceb87dcf289a644d8b6))

## [9.2.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v9.1.0...v9.2.0) (2025-05-22)


### Features

* **Preview:** add preview for topics ([#2292](https://github.com/ydb-platform/ydb-embedded-ui/issues/2292)) ([c4a4abf](https://github.com/ydb-platform/ydb-embedded-ui/commit/c4a4abf6119728abadad0a958afaaea4bba40309))
* **Tenants:** add onEditDB to uiFactory, add edit button ([#2305](https://github.com/ydb-platform/ydb-embedded-ui/issues/2305)) ([21c7126](https://github.com/ydb-platform/ydb-embedded-ui/commit/21c7126f147631b9c75362efa72f749a51d91d8b))


### Bug Fixes

* **Cluster:** autorefresh cluster data ([#2307](https://github.com/ydb-platform/ydb-embedded-ui/issues/2307)) ([713315c](https://github.com/ydb-platform/ydb-embedded-ui/commit/713315c9029f108024503a9089b47dcc7b2d0608))
* **Cluster:** side paddings ([#2301](https://github.com/ydb-platform/ydb-embedded-ui/issues/2301)) ([478c9e8](https://github.com/ydb-platform/ydb-embedded-ui/commit/478c9e8a5775ef1cc327dc46afe9402879721013))

## [9.1.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v9.0.0...v9.1.0) (2025-05-21)


### Features

* **Diagnostics:** split CDC and its implementation ([#2281](https://github.com/ydb-platform/ydb-embedded-ui/issues/2281)) ([9308686](https://github.com/ydb-platform/ydb-embedded-ui/commit/93086860924006bdf117ab941c6e57a718e56d29))
* **TopicData:** add topic message details in Drawer ([#2266](https://github.com/ydb-platform/ydb-embedded-ui/issues/2266)) ([025d5ff](https://github.com/ydb-platform/ydb-embedded-ui/commit/025d5ffcf1a3c7a11528a63ad22d9b923469af77))
* **vDisk:** allow evict vDisk by id ([#2267](https://github.com/ydb-platform/ydb-embedded-ui/issues/2267)) ([410d4d4](https://github.com/ydb-platform/ydb-embedded-ui/commit/410d4d4d6f1168b9ae405de88e9753b9c7807626))


### Bug Fixes

* **ClusterInfo:** increase fonts ([#2299](https://github.com/ydb-platform/ydb-embedded-ui/issues/2299)) ([345cbfb](https://github.com/ydb-platform/ydb-embedded-ui/commit/345cbfb0186907c98698f109f2669d44f35017ec))
* **QueryResultViewer:** change tabs order ([#2303](https://github.com/ydb-platform/ydb-embedded-ui/issues/2303)) ([1beef31](https://github.com/ydb-platform/ydb-embedded-ui/commit/1beef31bea69f0c2fd22ab6e8195638b3ee6bbbc))
* **Tenants:** do not add empty search to query ([#2287](https://github.com/ydb-platform/ydb-embedded-ui/issues/2287)) ([2664ec7](https://github.com/ydb-platform/ydb-embedded-ui/commit/2664ec7d96bc0352a62b3ef201f8710a35c499e1))

## [9.0.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v8.26.2...v9.0.0) (2025-05-13)


### ⚠ BREAKING CHANGES

* use uiFactory for logs and monitoring links ([#2274](https://github.com/ydb-platform/ydb-embedded-ui/issues/2274))
* remove internal-specific solution ([#2261](https://github.com/ydb-platform/ydb-embedded-ui/issues/2261))

### Features

* add import/s3 to operations ([#2256](https://github.com/ydb-platform/ydb-embedded-ui/issues/2256)) ([fb07e1c](https://github.com/ydb-platform/ydb-embedded-ui/commit/fb07e1c55896f632394e97c79afe5c9a9dbe496f))
* side panel aka refrigerator for query text in top queries ([#2134](https://github.com/ydb-platform/ydb-embedded-ui/issues/2134)) ([e2c269a](https://github.com/ydb-platform/ydb-embedded-ui/commit/e2c269a1f721bd1352b3599f1115e98fe76c1b11))


### Bug Fixes

* dead tablets on node page ([#2263](https://github.com/ydb-platform/ydb-embedded-ui/issues/2263)) ([986e6ab](https://github.com/ydb-platform/ydb-embedded-ui/commit/986e6ab9384611e3d8bfd4b39d4bc8e11f721d01))
* **Drawer:** handle clicks in components inside Drawer ([#2264](https://github.com/ydb-platform/ydb-embedded-ui/issues/2264)) ([b48bf24](https://github.com/ydb-platform/ydb-embedded-ui/commit/b48bf24691695c5cc023cbf0510255740faea27d))
* floor disk usage values ([#2253](https://github.com/ydb-platform/ydb-embedded-ui/issues/2253)) ([c10b3ff](https://github.com/ydb-platform/ydb-embedded-ui/commit/c10b3ff6eeaad8b78d5e4519def4d39b8031f4d5))
* handle gaps in offsets ([#2252](https://github.com/ydb-platform/ydb-embedded-ui/issues/2252)) ([5e75a0b](https://github.com/ydb-platform/ydb-embedded-ui/commit/5e75a0bd93b996b32063791aa6a6048200677111))
* missed schema value ([#2259](https://github.com/ydb-platform/ydb-embedded-ui/issues/2259)) ([33a4973](https://github.com/ydb-platform/ydb-embedded-ui/commit/33a497356740b53f07babd5c1ab1cc5c4647b7a6))
* red status rename to Critical ([#2270](https://github.com/ydb-platform/ydb-embedded-ui/issues/2270)) ([4006a27](https://github.com/ydb-platform/ydb-embedded-ui/commit/4006a277ab360d42a1a512c1367381284cc018e6))
* remove internal-specific solution ([#2261](https://github.com/ydb-platform/ydb-embedded-ui/issues/2261)) ([816a712](https://github.com/ydb-platform/ydb-embedded-ui/commit/816a7125635006c4a7efb2c0e6c0c92af872c8f8))
* **Tenants:** make state column wider ([#2276](https://github.com/ydb-platform/ydb-embedded-ui/issues/2276)) ([404f72c](https://github.com/ydb-platform/ydb-embedded-ui/commit/404f72cad19b690fc7a0f2cc2d6b6cef972b16b3))
* **Tenants:** move create db button to the right ([#2275](https://github.com/ydb-platform/ydb-embedded-ui/issues/2275)) ([82f32d9](https://github.com/ydb-platform/ydb-embedded-ui/commit/82f32d96ed868788b0e010318faddfce0a3996d1))
* use uiFactory for logs and monitoring links ([#2274](https://github.com/ydb-platform/ydb-embedded-ui/issues/2274)) ([974775f](https://github.com/ydb-platform/ydb-embedded-ui/commit/974775f76ecab425ae568670b276148f2cb10cdb))

## [8.26.2](https://github.com/ydb-platform/ydb-embedded-ui/compare/v8.26.1...v8.26.2) (2025-05-05)


### Bug Fixes

* **ClusterOverview:** loading state ([#2250](https://github.com/ydb-platform/ydb-embedded-ui/issues/2250)) ([1ad2ef1](https://github.com/ydb-platform/ydb-embedded-ui/commit/1ad2ef1c8772bcd73663f7c6912ed27335bdafe1))
* paginatedTable - calculate visible range on resize ([#2248](https://github.com/ydb-platform/ydb-embedded-ui/issues/2248)) ([e196fae](https://github.com/ydb-platform/ydb-embedded-ui/commit/e196fae039118054c24e5c7b9b1f54f5fe673648))

## [8.26.1](https://github.com/ydb-platform/ydb-embedded-ui/compare/v8.26.0...v8.26.1) (2025-05-05)


### Bug Fixes

* add cry cat svg ([#2246](https://github.com/ydb-platform/ydb-embedded-ui/issues/2246)) ([3f32315](https://github.com/ydb-platform/ydb-embedded-ui/commit/3f3231584e4736e3f4199884d5390c911607eebf))

## [8.26.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v8.25.0...v8.26.0) (2025-05-05)


### Features

* **Cluster:** redesign cluster dashboard ([#2176](https://github.com/ydb-platform/ydb-embedded-ui/issues/2176)) ([714e7c7](https://github.com/ydb-platform/ydb-embedded-ui/commit/714e7c7982e4a775ee8b4253f0ca39c7929a4cc1))
* **Drawer:** add utility to render header ([#2235](https://github.com/ydb-platform/ydb-embedded-ui/issues/2235)) ([c01b536](https://github.com/ydb-platform/ydb-embedded-ui/commit/c01b53625ddf92a325ed9eab5ca72f8fae0b07cb))
* save sorting columns in queries ([#2234](https://github.com/ydb-platform/ydb-embedded-ui/issues/2234)) ([dea22d1](https://github.com/ydb-platform/ydb-embedded-ui/commit/dea22d130ba568376692be67343a701b6d2615b2))
* **TopicData:** add tab for topic data ([#2145](https://github.com/ydb-platform/ydb-embedded-ui/issues/2145)) ([4c25054](https://github.com/ydb-platform/ydb-embedded-ui/commit/4c25054a2e20afe94d611dd788fdabac7c789f25))


### Bug Fixes

* **Cluster:** layout for tabs content ([#2219](https://github.com/ydb-platform/ydb-embedded-ui/issues/2219)) ([cc454d6](https://github.com/ydb-platform/ydb-embedded-ui/commit/cc454d65f56dcac67d7e9e2e52163965a0f7a084))
* columns width ([#2229](https://github.com/ydb-platform/ydb-embedded-ui/issues/2229)) ([c0b3efe](https://github.com/ydb-platform/ydb-embedded-ui/commit/c0b3efecf7c1e7bf4602d4ee3f24c002dda860f2))
* count pdisk-vdisk column width for skeletons ([#2214](https://github.com/ydb-platform/ydb-embedded-ui/issues/2214)) ([5727c21](https://github.com/ydb-platform/ydb-embedded-ui/commit/5727c218c0b62040eb8724bfbfe392d335971fac))
* **Drawer:** header styles ([#2242](https://github.com/ydb-platform/ydb-embedded-ui/issues/2242)) ([12214a8](https://github.com/ydb-platform/ydb-embedded-ui/commit/12214a8bb8698304e809557c99a765892040208a))
* fallback if unipika convert json failed ([#2227](https://github.com/ydb-platform/ydb-embedded-ui/issues/2227)) ([7887369](https://github.com/ydb-platform/ydb-embedded-ui/commit/788736938681153ec177007dc89ddd32741bbf0b))
* fix slashes everywhere ([#2244](https://github.com/ydb-platform/ydb-embedded-ui/issues/2244)) ([7535dbf](https://github.com/ydb-platform/ydb-embedded-ui/commit/7535dbf00183d8dfa630089a6607bb08f4e22e62))
* **PaginatedStorage:** controls should be fixed ([#2217](https://github.com/ydb-platform/ydb-embedded-ui/issues/2217)) ([c12fed4](https://github.com/ydb-platform/ydb-embedded-ui/commit/c12fed49d6a8687212f8a9382fe4dcf0242b5430))
* **StatusIcon:** add icons for grey and green statuses ([#2203](https://github.com/ydb-platform/ydb-embedded-ui/issues/2203)) ([9c77257](https://github.com/ydb-platform/ydb-embedded-ui/commit/9c7725767977464b01970e06bc2b068144ec483c))

## [8.25.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v8.24.1...v8.25.0) (2025-04-25)


### Features

* add endpoint to connect to db code snippets ([#2198](https://github.com/ydb-platform/ydb-embedded-ui/issues/2198)) ([6e45802](https://github.com/ydb-platform/ydb-embedded-ui/commit/6e45802dc05baf3fe9ff5cef26e396964b8d827a))


### Bug Fixes

* find out performance issue with big tables of nodes ([#2206](https://github.com/ydb-platform/ydb-embedded-ui/issues/2206)) ([5c22404](https://github.com/ydb-platform/ydb-embedded-ui/commit/5c2240477ef132c9b58ca96b2ae1549f6e410e3b))

## [8.24.1](https://github.com/ydb-platform/ydb-embedded-ui/compare/v8.24.0...v8.24.1) (2025-04-23)


### Bug Fixes

* fix top shards path column and better overriding for advisor ([#2197](https://github.com/ydb-platform/ydb-embedded-ui/issues/2197)) ([57df88b](https://github.com/ydb-platform/ydb-embedded-ui/commit/57df88b27fc1e3315f1a901dd4ec7eaecbaf6fae))

## [8.24.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v8.23.0...v8.24.0) (2025-04-22)


### Features

* add possibility to pass external columns ([#2187](https://github.com/ydb-platform/ydb-embedded-ui/issues/2187)) ([65b2e72](https://github.com/ydb-platform/ydb-embedded-ui/commit/65b2e72bd0516609257120b1b44d38f4336e368e))

## [8.23.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v8.22.1...v8.23.0) (2025-04-22)


### Features

* enable basename for multi cluster version ([#2153](https://github.com/ydb-platform/ydb-embedded-ui/issues/2153)) ([91963b9](https://github.com/ydb-platform/ydb-embedded-ui/commit/91963b9e256be20682fbb0cbe9ae26b8d8f567a9))


### Bug Fixes

* display query settings banner for non-default api call or txmode ([#2152](https://github.com/ydb-platform/ydb-embedded-ui/issues/2152)) ([395efc7](https://github.com/ydb-platform/ydb-embedded-ui/commit/395efc718645762bf4b0ddf2b419035790bb94a1))
* fix wrong pdisk id order ([#2170](https://github.com/ydb-platform/ydb-embedded-ui/issues/2170)) ([c840b21](https://github.com/ydb-platform/ydb-embedded-ui/commit/c840b21d3d95441202ac7a97472fc556c7b50d04))
* normalizePathSlashes should normalize multiple leading slashes ([#2186](https://github.com/ydb-platform/ydb-embedded-ui/issues/2186)) ([3e09755](https://github.com/ydb-platform/ydb-embedded-ui/commit/3e097553a949e85c3e779594207dc45d4876e465))
* table in queries sorts by string values after backend sort ([#2183](https://github.com/ydb-platform/ydb-embedded-ui/issues/2183)) ([fc66fbe](https://github.com/ydb-platform/ydb-embedded-ui/commit/fc66fbef10e105aa22c7cce235996e0d8f6f70b2))
* **TableGroup:** prevent content border overflow ([#2166](https://github.com/ydb-platform/ydb-embedded-ui/issues/2166)) ([7cefb6d](https://github.com/ydb-platform/ydb-embedded-ui/commit/7cefb6d025c84cc3669cfa7704f7aaa199f6c077))
* **Tenants:** show create DB button only when table is loaded ([#2175](https://github.com/ydb-platform/ydb-embedded-ui/issues/2175)) ([b82e029](https://github.com/ydb-platform/ydb-embedded-ui/commit/b82e029c9040fc2ead47353e3992ce2d35179970))
* update PDisk errors colors ([#2171](https://github.com/ydb-platform/ydb-embedded-ui/issues/2171)) ([2bb7b2d](https://github.com/ydb-platform/ydb-embedded-ui/commit/2bb7b2dac480b080c4e730ebde6d50b0c930b5f0))
* **Versions:** should calculate minor version to get color ([#2180](https://github.com/ydb-platform/ydb-embedded-ui/issues/2180)) ([f77f933](https://github.com/ydb-platform/ydb-embedded-ui/commit/f77f933e3a47c772e5795c6c8546fb2ae4e18818))

## [8.22.1](https://github.com/ydb-platform/ydb-embedded-ui/compare/v8.22.0...v8.22.1) (2025-04-15)


### Bug Fixes

* update autorefresh if more that autorefresh in inactive tab passed ([#2140](https://github.com/ydb-platform/ydb-embedded-ui/issues/2140)) ([c5bd2ac](https://github.com/ydb-platform/ydb-embedded-ui/commit/c5bd2ac759ae4729497106b258b20f529a471b7c))
* useHotkeysPanel hook ([#2151](https://github.com/ydb-platform/ydb-embedded-ui/issues/2151)) ([08b39d1](https://github.com/ydb-platform/ydb-embedded-ui/commit/08b39d11d1d7b5a66993001867c7e4fb6df93fcb))

## [8.22.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v8.21.0...v8.22.0) (2025-04-11)


### Features

* **Tenants:** correct links with relative path in balancer ([#2125](https://github.com/ydb-platform/ydb-embedded-ui/issues/2125)) ([f427375](https://github.com/ydb-platform/ydb-embedded-ui/commit/f42737575b20f3cdf80af98e80f43666443bedd3))

## [8.21.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v8.20.1...v8.21.0) (2025-04-09)


### Features

* **Clusters:** correct links with relative path in balancer ([#2121](https://github.com/ydb-platform/ydb-embedded-ui/issues/2121)) ([4776c0c](https://github.com/ydb-platform/ydb-embedded-ui/commit/4776c0c118df42ef11d6790c8f8348d5dabe937c))
* design query tab ([#2077](https://github.com/ydb-platform/ydb-embedded-ui/issues/2077)) ([06d243d](https://github.com/ydb-platform/ydb-embedded-ui/commit/06d243dfa5aefdb46001f2d9a78be2218a929869))
* do not automatically refresh content when browser tab is inactive ([#2122](https://github.com/ydb-platform/ydb-embedded-ui/issues/2122)) ([341c0d8](https://github.com/ydb-platform/ydb-embedded-ui/commit/341c0d8c337a944d582649d8aee1b9f6e34fd757))
* make keyboard shortcuts help page ([#2116](https://github.com/ydb-platform/ydb-embedded-ui/issues/2116)) ([b401559](https://github.com/ydb-platform/ydb-embedded-ui/commit/b401559355a51f18d14203e146c59af90aad25d6))


### Bug Fixes

* reuse table schema for select template for current table ([#2127](https://github.com/ydb-platform/ydb-embedded-ui/issues/2127)) ([b577ee5](https://github.com/ydb-platform/ydb-embedded-ui/commit/b577ee5fe60f220d322948a43d17568a19e36596))
* try no unsaved for templates ([#2117](https://github.com/ydb-platform/ydb-embedded-ui/issues/2117)) ([e7d4a50](https://github.com/ydb-platform/ydb-embedded-ui/commit/e7d4a50f4ebf9f46567d48a9ef56683b9395e8b7))
* use blue color only for not replicated vdisks ([#2110](https://github.com/ydb-platform/ydb-embedded-ui/issues/2110)) ([2e6051b](https://github.com/ydb-platform/ydb-embedded-ui/commit/2e6051bc4448951945594f0982d6899d338c004a))

## [8.20.1](https://github.com/ydb-platform/ydb-embedded-ui/compare/v8.20.0...v8.20.1) (2025-04-04)


### Bug Fixes

* add RealNumberOfCpus to aggregates ([#2095](https://github.com/ydb-platform/ydb-embedded-ui/issues/2095)) ([b0453d5](https://github.com/ydb-platform/ydb-embedded-ui/commit/b0453d56ff19e61a4a0c003249aa89a46586071c))

## [8.20.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v8.19.1...v8.20.0) (2025-04-03)


### Features

* allow pass create and delete DB funcs to UI ([#2087](https://github.com/ydb-platform/ydb-embedded-ui/issues/2087)) ([810f60b](https://github.com/ydb-platform/ydb-embedded-ui/commit/810f60b46ad75ad184c43d61ce4fa65504f29ee2))

## [8.19.1](https://github.com/ydb-platform/ydb-embedded-ui/compare/v8.19.0...v8.19.1) (2025-04-03)


### Bug Fixes

* fake commit for release to build ([#2088](https://github.com/ydb-platform/ydb-embedded-ui/issues/2088)) ([3b97fa7](https://github.com/ydb-platform/ydb-embedded-ui/commit/3b97fa77c06a7bfb2a71ed18ebbb8b7e397ffe52))

## [8.19.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v8.18.0...v8.19.0) (2025-04-02)


### Features

* add new popup menu icon ([#2045](https://github.com/ydb-platform/ydb-embedded-ui/issues/2045)) ([21de77f](https://github.com/ydb-platform/ydb-embedded-ui/commit/21de77faad169cb1a2890e480a3b860d8d2ef8aa))
* add support for RealNumberOfCpus for load average ([#2082](https://github.com/ydb-platform/ydb-embedded-ui/issues/2082)) ([0bad262](https://github.com/ydb-platform/ydb-embedded-ui/commit/0bad262ff3befee4039956f2588bc0a61a22b03c))


### Bug Fixes

* **PDiskPage:** fix error boundary on failed restart ([#2069](https://github.com/ydb-platform/ydb-embedded-ui/issues/2069)) ([4624845](https://github.com/ydb-platform/ydb-embedded-ui/commit/4624845577d32d8617221892bb9d5e79871315de))
* **PDiskSpaceDistribution:** use only space severity for slots ([#2070](https://github.com/ydb-platform/ydb-embedded-ui/issues/2070)) ([4ea21a1](https://github.com/ydb-platform/ydb-embedded-ui/commit/4ea21a156f07fb206629a82bfc143394a3ebe8aa))
* shards table dissapeared ([#2072](https://github.com/ydb-platform/ydb-embedded-ui/issues/2072)) ([6ea6cd5](https://github.com/ydb-platform/ydb-embedded-ui/commit/6ea6cd55f71e6cd7837409c41ffec64d68622773))
* stream test in safari ([#2059](https://github.com/ydb-platform/ydb-embedded-ui/issues/2059)) ([74355a4](https://github.com/ydb-platform/ydb-embedded-ui/commit/74355a430712c7970b56dd18852673075e692d7d))

## [8.18.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v8.17.0...v8.18.0) (2025-03-27)


### Features

* show transfer errors in UI ([#2043](https://github.com/ydb-platform/ydb-embedded-ui/issues/2043)) ([cb5a755](https://github.com/ydb-platform/ydb-embedded-ui/commit/cb5a755a553b1eaa946f69d5f7a9e1b24700863d))
* turn streaming on by default ([#2028](https://github.com/ydb-platform/ydb-embedded-ui/issues/2028)) ([bc5d59d](https://github.com/ydb-platform/ydb-embedded-ui/commit/bc5d59d09d577b2bfc6661a1a667ffbb1f2ffa95))


### Bug Fixes

* **Versions:** loading state ([#2050](https://github.com/ydb-platform/ydb-embedded-ui/issues/2050)) ([66cbd6f](https://github.com/ydb-platform/ydb-embedded-ui/commit/66cbd6f3a05a519df0afe0dd591cd310999d1280))
* whole tablet family for tabletPage ([#2054](https://github.com/ydb-platform/ydb-embedded-ui/issues/2054)) ([98bfb54](https://github.com/ydb-platform/ydb-embedded-ui/commit/98bfb540ff20f473d9a565c91811d07b2ef398d5))

## [8.17.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v8.16.0...v8.17.0) (2025-03-26)


### Features

* followerId for tablets ([#2025](https://github.com/ydb-platform/ydb-embedded-ui/issues/2025)) ([63d5afd](https://github.com/ydb-platform/ydb-embedded-ui/commit/63d5afd64754df291e0e5eec90142a37114ebf85))
* parse logging link as default value ([#2036](https://github.com/ydb-platform/ydb-embedded-ui/issues/2036)) ([74f7a58](https://github.com/ydb-platform/ydb-embedded-ui/commit/74f7a5892a08a6a61cd6eff1bfe3acde9aac7f6e))


### Bug Fixes

* add required field SubDomainKey to getNodes ([#2047](https://github.com/ydb-platform/ydb-embedded-ui/issues/2047)) ([cf1ddc3](https://github.com/ydb-platform/ydb-embedded-ui/commit/cf1ddc39225ada474d858f3ae8eb1f1f595da068))
* supported changes of the transfer configuration structure ([#2032](https://github.com/ydb-platform/ydb-embedded-ui/issues/2032)) ([78965ed](https://github.com/ydb-platform/ydb-embedded-ui/commit/78965edd4caf013ec4a2aca91614cef7550a9f16))

## [8.16.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v8.15.0...v8.16.0) (2025-03-20)


### Features

* add database hyperlink to logging service ([#2021](https://github.com/ydb-platform/ydb-embedded-ui/issues/2021)) ([dbab46b](https://github.com/ydb-platform/ydb-embedded-ui/commit/dbab46b2d688b25669f07b2aa7294598eb5443d2))
* query streaming only for queryService ([#2015](https://github.com/ydb-platform/ydb-embedded-ui/issues/2015)) ([105fd2c](https://github.com/ydb-platform/ydb-embedded-ui/commit/105fd2c5a706bf886c6457a78a805e374df6a67c))
* request clusters handler only on Versions tab ([#2008](https://github.com/ydb-platform/ydb-embedded-ui/issues/2008)) ([7aacdfe](https://github.com/ydb-platform/ydb-embedded-ui/commit/7aacdfe8e939e730d2a1dbc10a7c7fa785f54bbb))


### Bug Fixes

* kind export operations type ([#2030](https://github.com/ydb-platform/ydb-embedded-ui/issues/2030)) ([3b47eb5](https://github.com/ydb-platform/ydb-embedded-ui/commit/3b47eb59b5444f3f3bff5425f08f3d132c251446))
* **ObjectSummary:** do not display CreateTime if CreateStep is 0 ([#2018](https://github.com/ydb-platform/ydb-embedded-ui/issues/2018)) ([7af1ed3](https://github.com/ydb-platform/ydb-embedded-ui/commit/7af1ed377fe47fb38498dd7540f8c273a8a9b0be))
* **ShemaViewer:** show loader correctly ([#2019](https://github.com/ydb-platform/ydb-embedded-ui/issues/2019)) ([29ae340](https://github.com/ydb-platform/ydb-embedded-ui/commit/29ae340156be237a7e01ff429bc62fa38408c63a))
* unsaved changes in query editor ([#2026](https://github.com/ydb-platform/ydb-embedded-ui/issues/2026)) ([d1d64f7](https://github.com/ydb-platform/ydb-embedded-ui/commit/d1d64f7792842abf42de9c72de3b50c8c3bde1ec))
* **VDiskInfo:** lowercase vdisk page ([#2014](https://github.com/ydb-platform/ydb-embedded-ui/issues/2014)) ([577c9aa](https://github.com/ydb-platform/ydb-embedded-ui/commit/577c9aa2f545e4792fac6fd299dea9f8653a9082))

## [8.15.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v8.14.0...v8.15.0) (2025-03-10)


### Features

* add QUERY_TECHNICAL_MARK to all UI queries ([#1992](https://github.com/ydb-platform/ydb-embedded-ui/issues/1992)) ([6d53518](https://github.com/ydb-platform/ydb-embedded-ui/commit/6d535186247307f40b01386a850bf30ffb9ffb93))
* add ShardsTable to componentsRegistry ([#1993](https://github.com/ydb-platform/ydb-embedded-ui/issues/1993)) ([a390679](https://github.com/ydb-platform/ydb-embedded-ui/commit/a390679234d1085b5e19e1933691a60414a450d8))
* **YDBSyntaxHighlighter:** separate component, load languages on demand ([#2004](https://github.com/ydb-platform/ydb-embedded-ui/issues/2004)) ([a544def](https://github.com/ydb-platform/ydb-embedded-ui/commit/a544deff6db5539f5622111519f770a2c474377b))

## [8.14.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v8.13.0...v8.14.0) (2025-03-07)


### Features

* create Api directly with YdbEmbeddedAPI class ([#1998](https://github.com/ydb-platform/ydb-embedded-ui/issues/1998)) ([b139eb5](https://github.com/ydb-platform/ydb-embedded-ui/commit/b139eb5fe5ac62175227b63dceac8e9889bdb588))
* support transfer ([#1995](https://github.com/ydb-platform/ydb-embedded-ui/issues/1995)) ([966ff4b](https://github.com/ydb-platform/ydb-embedded-ui/commit/966ff4b6fc5e32ae7fafc4e865949bb02c3a5f01))
* **TopShards:** colorize usage ([#2003](https://github.com/ydb-platform/ydb-embedded-ui/issues/2003)) ([dde82b0](https://github.com/ydb-platform/ydb-embedded-ui/commit/dde82b0769fa728b4b22f09ce6b40df4a9f86a15))


### Bug Fixes

* fix undefined process in package ([#1997](https://github.com/ydb-platform/ydb-embedded-ui/issues/1997)) ([0b6b99d](https://github.com/ydb-platform/ydb-embedded-ui/commit/0b6b99d2bcd0d3d00af40c01beff095ba8ae4d7e))
* **LinkWithIcon:** make inline ([#2002](https://github.com/ydb-platform/ydb-embedded-ui/issues/2002)) ([290bb18](https://github.com/ydb-platform/ydb-embedded-ui/commit/290bb183a98a21e74b49ae419cd64ed10074728e))
* **PDiskInfo:** display SharedWithOs only when true ([#1991](https://github.com/ydb-platform/ydb-embedded-ui/issues/1991)) ([7c21d71](https://github.com/ydb-platform/ydb-embedded-ui/commit/7c21d71c6fcf272da2329977be1af9671913372e))
* **TopShards:** display table for column entities ([#1999](https://github.com/ydb-platform/ydb-embedded-ui/issues/1999)) ([6b28803](https://github.com/ydb-platform/ydb-embedded-ui/commit/6b2880321f437a40be52f0b78207390c9019ad93))

## [8.13.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v8.12.0...v8.13.0) (2025-02-28)


### Features

* **ErrorBoundary:** rework error page ([#1983](https://github.com/ydb-platform/ydb-embedded-ui/issues/1983)) ([4a97195](https://github.com/ydb-platform/ydb-embedded-ui/commit/4a97195befe991b6204752d175519ae2d5afaebd))
* make links from disk pop-up better ([#1986](https://github.com/ydb-platform/ydb-embedded-ui/issues/1986)) ([336d74b](https://github.com/ydb-platform/ydb-embedded-ui/commit/336d74b95502e5d6f2e508bce979a8d02521a64b))
* redesign query ([#1974](https://github.com/ydb-platform/ydb-embedded-ui/issues/1974)) ([9ac8cfc](https://github.com/ydb-platform/ydb-embedded-ui/commit/9ac8cfc46c71b38aceae2f1ed8b84113b7fbf90b))
* resource pool – add proper icon ([#1982](https://github.com/ydb-platform/ydb-embedded-ui/issues/1982)) ([9c0c6f8](https://github.com/ydb-platform/ydb-embedded-ui/commit/9c0c6f8ce93e44920c860eb19d961b209d469f7b))


### Bug Fixes

* rename tabs in query result ([#1987](https://github.com/ydb-platform/ydb-embedded-ui/issues/1987)) ([b9d9f0b](https://github.com/ydb-platform/ydb-embedded-ui/commit/b9d9f0be128537b30117922274b16f477d491205))
* tests ([#1988](https://github.com/ydb-platform/ydb-embedded-ui/issues/1988)) ([519f587](https://github.com/ydb-platform/ydb-embedded-ui/commit/519f5873ffa5eecb031e71b34206140ae1e02859))

## [8.12.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v8.11.1...v8.12.0) (2025-02-25)


### Features

* add database to authentication process ([#1976](https://github.com/ydb-platform/ydb-embedded-ui/issues/1976)) ([d9067b3](https://github.com/ydb-platform/ydb-embedded-ui/commit/d9067b373f832083c537eb35ca51ab6e56853552))
* hide some columns and storage nodes for users-viewers ([#1967](https://github.com/ydb-platform/ydb-embedded-ui/issues/1967)) ([249011d](https://github.com/ydb-platform/ydb-embedded-ui/commit/249011d93bff8a20e71ea48504f8c4a4367ff33b))
* support multipart responses in query ([#1865](https://github.com/ydb-platform/ydb-embedded-ui/issues/1865)) ([99ee997](https://github.com/ydb-platform/ydb-embedded-ui/commit/99ee99713be9165ffd2140e3cab29ec26758b70f))
* **TabletsTable:** add search by id ([#1981](https://github.com/ydb-platform/ydb-embedded-ui/issues/1981)) ([d68adba](https://github.com/ydb-platform/ydb-embedded-ui/commit/d68adba4d239bbfaf35f006b25e337d1976e1ab5))


### Bug Fixes

* **Node:** fix developer ui link in dev mode ([#1979](https://github.com/ydb-platform/ydb-embedded-ui/issues/1979)) ([ad64c8c](https://github.com/ydb-platform/ydb-embedded-ui/commit/ad64c8c8e3aa951ec5f97ce6cbb68e9cb3b3254b))

## [8.11.1](https://github.com/ydb-platform/ydb-embedded-ui/compare/v8.11.0...v8.11.1) (2025-02-18)


### Bug Fixes

* **JsonViewer:** handle case sensitive search ([#1966](https://github.com/ydb-platform/ydb-embedded-ui/issues/1966)) ([f2aabb7](https://github.com/ydb-platform/ydb-embedded-ui/commit/f2aabb7041cd6cb445df78490b54df7d1dd0945b))
* unipika styles in one file ([#1963](https://github.com/ydb-platform/ydb-embedded-ui/issues/1963)) ([ff018a1](https://github.com/ydb-platform/ydb-embedded-ui/commit/ff018a1e90404c2d7600d832723b2c75824aa786))

## [8.11.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v8.10.0...v8.11.0) (2025-02-18)


### Features

* add new JsonViewer ([#1951](https://github.com/ydb-platform/ydb-embedded-ui/issues/1951)) ([61a8d48](https://github.com/ydb-platform/ydb-embedded-ui/commit/61a8d4882e2fff38e11bb33c09b3464da8a639fd))


### Bug Fixes

* make SLO uppercase ([#1959](https://github.com/ydb-platform/ydb-embedded-ui/issues/1959)) ([53e013c](https://github.com/ydb-platform/ydb-embedded-ui/commit/53e013c027d990100574463d5f9983206405ceef))
* **ObjectSummary:** refresh tabs with tree refresh, disable autorefresh ([#1946](https://github.com/ydb-platform/ydb-embedded-ui/issues/1946)) ([c28ade6](https://github.com/ydb-platform/ydb-embedded-ui/commit/c28ade6a94d63ff9fdd071d56b9970deff19e91b))
* **schemaQueryTemplates:** insert $ sign ([#1945](https://github.com/ydb-platform/ydb-embedded-ui/issues/1945)) ([267c445](https://github.com/ydb-platform/ydb-embedded-ui/commit/267c4451a0e1df3221e5db26905836360a9f39fc))
* **SchemaViewer:** fix sort order and add key icon ([#1957](https://github.com/ydb-platform/ydb-embedded-ui/issues/1957)) ([8b5221a](https://github.com/ydb-platform/ydb-embedded-ui/commit/8b5221a6bd284fb8d10930a1975c16c4d57a5eae))
* **Storage:** fix disks view for degraded group ([#1930](https://github.com/ydb-platform/ydb-embedded-ui/issues/1930)) ([a2b7d1c](https://github.com/ydb-platform/ydb-embedded-ui/commit/a2b7d1c74cf9933d179181e97885347c3a60d06c))

## [8.10.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v8.9.0...v8.10.0) (2025-02-12)


### Features

* **HealthcheckPreview:** manual fetch for ydb_ru ([#1937](https://github.com/ydb-platform/ydb-embedded-ui/issues/1937)) ([6b1cac8](https://github.com/ydb-platform/ydb-embedded-ui/commit/6b1cac892e8afa5f06c9cf6437c76fe7ba13ac18))


### Bug Fixes

* add monaco-yql-languages to peer deps ([#1932](https://github.com/ydb-platform/ydb-embedded-ui/issues/1932)) ([40b803c](https://github.com/ydb-platform/ydb-embedded-ui/commit/40b803cce83414078a22518ac6183ed0b544099b))

## [8.9.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v8.8.0...v8.9.0) (2025-02-11)


### Features

* move autocomplete logic to library ([#1909](https://github.com/ydb-platform/ydb-embedded-ui/issues/1909)) ([58a0f08](https://github.com/ydb-platform/ydb-embedded-ui/commit/58a0f08478f2b6c1a0bcecbbd3612e78c3bf0d80))
* **Node:** rework page ([#1917](https://github.com/ydb-platform/ydb-embedded-ui/issues/1917)) ([187032b](https://github.com/ydb-platform/ydb-embedded-ui/commit/187032b6cc77ffc60d925663766dd8e830f4d5d4))


### Bug Fixes

* code assistant option ([#1922](https://github.com/ydb-platform/ydb-embedded-ui/issues/1922)) ([38ea87f](https://github.com/ydb-platform/ydb-embedded-ui/commit/38ea87fe65e3eb33ee8287b213e65b32d0d20e48))
* **VDisk:** do not show vdisk as not replicated if no data ([#1921](https://github.com/ydb-platform/ydb-embedded-ui/issues/1921)) ([f648f08](https://github.com/ydb-platform/ydb-embedded-ui/commit/f648f0890b3e308a5875566a141be168302d3924))

## [8.8.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v8.7.0...v8.8.0) (2025-02-07)


### Features

* code assistant integration ([#1902](https://github.com/ydb-platform/ydb-embedded-ui/issues/1902)) ([a10a5cd](https://github.com/ydb-platform/ydb-embedded-ui/commit/a10a5cddaba18100bbc23a1b0267fd15417497b9))
* extend alter table query templates with enable auto split ([#1913](https://github.com/ydb-platform/ydb-embedded-ui/issues/1913)) ([cc81623](https://github.com/ydb-platform/ydb-embedded-ui/commit/cc81623ab9bc73b19ab24ae3d9367ff8d59a0344))


### Bug Fixes

* **Configs,Operations:** fix blicks on autorefresh ([#1908](https://github.com/ydb-platform/ydb-embedded-ui/issues/1908)) ([4d9c2b6](https://github.com/ydb-platform/ydb-embedded-ui/commit/4d9c2b6e5dbac9065e46794d96acbdddba66cfbf))
* create topic template ([#1910](https://github.com/ydb-platform/ydb-embedded-ui/issues/1910)) ([9ab321b](https://github.com/ydb-platform/ydb-embedded-ui/commit/9ab321bca3a3388059eeb7b7da014196bd00227a))
* **FormattedBytes:** show 1_000 with another unit ([#1901](https://github.com/ydb-platform/ydb-embedded-ui/issues/1901)) ([0e8bdd8](https://github.com/ydb-platform/ydb-embedded-ui/commit/0e8bdd8ea577f82be6d6b775b1f8ab22b771b4d9))
* **PDiskSpaceDistribution:** update slots severity calculation ([#1907](https://github.com/ydb-platform/ydb-embedded-ui/issues/1907)) ([0a49720](https://github.com/ydb-platform/ydb-embedded-ui/commit/0a497206b578f01af946fb53ebb969e83ebde5c1))
* remove trace polling ([#1915](https://github.com/ydb-platform/ydb-embedded-ui/issues/1915)) ([5541ca7](https://github.com/ydb-platform/ydb-embedded-ui/commit/5541ca7319a36c67fb9ca93bc5c36d4c2dc6f969))
* **SchemaViewer:** use partitioning keys order from HashColumns ([#1916](https://github.com/ydb-platform/ydb-embedded-ui/issues/1916)) ([fe3845c](https://github.com/ydb-platform/ydb-embedded-ui/commit/fe3845c2f8203f4d2ca570232f15f23b4622a215))

## [8.7.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v8.6.0...v8.7.0) (2025-01-31)


### Features

* **MetricChart:** add legend to some charts ([#1893](https://github.com/ydb-platform/ydb-embedded-ui/issues/1893)) ([b190e24](https://github.com/ydb-platform/ydb-embedded-ui/commit/b190e2461b6b3edecc272a804597415fbe0742f5))
* **SchemaViewer:** calculate column width based on data ([#1885](https://github.com/ydb-platform/ydb-embedded-ui/issues/1885)) ([85f19c3](https://github.com/ydb-platform/ydb-embedded-ui/commit/85f19c3d579a2d365056a69be43856a82bd5a9e4))


### Bug Fixes

* add additional timeout for flaky tests ([#1884](https://github.com/ydb-platform/ydb-embedded-ui/issues/1884)) ([9a502d2](https://github.com/ydb-platform/ydb-embedded-ui/commit/9a502d27e592522ee93f296be6b9047e7e87032e))
* avoid confusion with information interpretation ([#1871](https://github.com/ydb-platform/ydb-embedded-ui/issues/1871)) ([54c7091](https://github.com/ydb-platform/ydb-embedded-ui/commit/54c7091184d4cc1993bb735a4df9673fc2c32873))
* disable autorefresh for ydb_ru ([#1890](https://github.com/ydb-platform/ydb-embedded-ui/issues/1890)) ([a0ba20f](https://github.com/ydb-platform/ydb-embedded-ui/commit/a0ba20f68b288bb8be9fa4a12effeea1d00cd9b6))
* **Storage:** tune popups for vdisk and pdisk ([#1883](https://github.com/ydb-platform/ydb-embedded-ui/issues/1883)) ([ae115d9](https://github.com/ydb-platform/ydb-embedded-ui/commit/ae115d97dedf4dcb353cf3c53d0418131550bb19))
* **Tenant:** fix tabs reset on schema object change ([#1881](https://github.com/ydb-platform/ydb-embedded-ui/issues/1881)) ([4dd053d](https://github.com/ydb-platform/ydb-embedded-ui/commit/4dd053deb240823780a0c87aa88d3628631f26da))

## [8.6.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v8.5.0...v8.6.0) (2025-01-27)


### Features

* add connect to DB dialog ([#1838](https://github.com/ydb-platform/ydb-embedded-ui/issues/1838)) ([ba22a39](https://github.com/ydb-platform/ydb-embedded-ui/commit/ba22a39e20f6417a9f24a2c0f5084cb63c4cf759))
* pass database to whoami ([#1860](https://github.com/ydb-platform/ydb-embedded-ui/issues/1860)) ([1990103](https://github.com/ydb-platform/ydb-embedded-ui/commit/19901033fef639450c729e398fb73c75e4cacc66))
* **SchemaTree:** do not expand childless nodes ([#1868](https://github.com/ydb-platform/ydb-embedded-ui/issues/1868)) ([ae8aa6d](https://github.com/ydb-platform/ydb-embedded-ui/commit/ae8aa6d0e75b42609e51612a12594d0ab7b2bdcf))
* **Storage:** add State column ([#1859](https://github.com/ydb-platform/ydb-embedded-ui/issues/1859)) ([cda185b](https://github.com/ydb-platform/ydb-embedded-ui/commit/cda185bb35ec4c860b737bd407dd50af96bb31fe))


### Bug Fixes

* **QueryEditor:** dont render Results every time editor input changes ([#1879](https://github.com/ydb-platform/ydb-embedded-ui/issues/1879)) ([4db34be](https://github.com/ydb-platform/ydb-embedded-ui/commit/4db34be1f6a30054feb9674d69ef9f80e57c112f))
* **SchemaTree:** expand nodes if ChildrenExist is undefined ([#1872](https://github.com/ydb-platform/ydb-embedded-ui/issues/1872)) ([2af9d9e](https://github.com/ydb-platform/ydb-embedded-ui/commit/2af9d9e3ed075d8b457748d8c80e48947c07ca0b))
* **UptimeViewer:** do not show StartTime if DisconnectTime present ([#1864](https://github.com/ydb-platform/ydb-embedded-ui/issues/1864)) ([36038bc](https://github.com/ydb-platform/ydb-embedded-ui/commit/36038bcfb7a463a32d49c3c20d5ca29ae615a552))
* use eye icon for preview ([#1873](https://github.com/ydb-platform/ydb-embedded-ui/issues/1873)) ([c11e616](https://github.com/ydb-platform/ydb-embedded-ui/commit/c11e6165791b8c7258aca5ab9f972b2360cbfd21))
* **VDisks:** use fixed VDisk width ([#1857](https://github.com/ydb-platform/ydb-embedded-ui/issues/1857)) ([613bbf6](https://github.com/ydb-platform/ydb-embedded-ui/commit/613bbf60600bb802aba2338db78b86044e1a0e1c))

## [8.5.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v8.4.0...v8.5.0) (2025-01-22)


### Features

* **ClusterInfo:** add slo logs link ([#1850](https://github.com/ydb-platform/ydb-embedded-ui/issues/1850)) ([7e0429b](https://github.com/ydb-platform/ydb-embedded-ui/commit/7e0429b632a73a79051a366c94224e7333d84f05))
* download dump with all info ([#1862](https://github.com/ydb-platform/ydb-embedded-ui/issues/1862)) ([f09cbe9](https://github.com/ydb-platform/ydb-embedded-ui/commit/f09cbe9dce9634cfa1f001d428d48837ba58df1c))
* move to node 20, update deps ([#1792](https://github.com/ydb-platform/ydb-embedded-ui/issues/1792)) ([33ab5de](https://github.com/ydb-platform/ydb-embedded-ui/commit/33ab5de4c6e0207490a0077ffdd220163714c32b))
* **QueryEditor:** add error highlighting ([#1833](https://github.com/ydb-platform/ydb-embedded-ui/issues/1833)) ([b89d084](https://github.com/ydb-platform/ydb-embedded-ui/commit/b89d084c5999d82baf080bf1228c4a7ad30b6afc))
* **Storage:** group disks by DC ([#1823](https://github.com/ydb-platform/ydb-embedded-ui/issues/1823)) ([dec5b95](https://github.com/ydb-platform/ydb-embedded-ui/commit/dec5b9592ed3b491e5073a5b8903eef65be4dc1d))
* use the same thresholds for all progress bars ([#1820](https://github.com/ydb-platform/ydb-embedded-ui/issues/1820)) ([1b74502](https://github.com/ydb-platform/ydb-embedded-ui/commit/1b74502de4cf91f8babb9349c6740cf11e63a238))


### Bug Fixes

* **EntityStatus:** background color for button under hover ([#1842](https://github.com/ydb-platform/ydb-embedded-ui/issues/1842)) ([5c722dd](https://github.com/ydb-platform/ydb-embedded-ui/commit/5c722dd461049a4f4719eadd915f29d063cf2f1d))
* hide dev ui links for users with viewer rights ([#1824](https://github.com/ydb-platform/ydb-embedded-ui/issues/1824)) ([093e79a](https://github.com/ydb-platform/ydb-embedded-ui/commit/093e79a466104b4dc8c049f705d7f0324a6b79df))
* **OperationCell:** increase font weight for operation name ([#1825](https://github.com/ydb-platform/ydb-embedded-ui/issues/1825)) ([2e618f4](https://github.com/ydb-platform/ydb-embedded-ui/commit/2e618f42b97f074a1a7b2dcba0d10da186d5b6da))
* remove retries ([#1829](https://github.com/ydb-platform/ydb-embedded-ui/issues/1829)) ([3cd7364](https://github.com/ydb-platform/ydb-embedded-ui/commit/3cd73641351ad32fb0b8e650386a989a04ff9769))
* **Storage:** fix encryption label shrink ([#1851](https://github.com/ydb-platform/ydb-embedded-ui/issues/1851)) ([bb6d7be](https://github.com/ydb-platform/ydb-embedded-ui/commit/bb6d7bed75f4cdb7a401022100099d65e855c5e6))
* update actions ([#1834](https://github.com/ydb-platform/ydb-embedded-ui/issues/1834)) ([e3c2243](https://github.com/ydb-platform/ydb-embedded-ui/commit/e3c224302b0e7492069b00271dcc53f10f9fc68d))
* **UserSettings:** show description under setting control ([#1827](https://github.com/ydb-platform/ydb-embedded-ui/issues/1827)) ([45ddc7e](https://github.com/ydb-platform/ydb-embedded-ui/commit/45ddc7ea912be8abe38d40cfa0f0578175177fe7))

## [8.4.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v8.3.0...v8.4.0) (2025-01-13)


### Features

* **autocomplete:** suggest columns from subqueries ([#1788](https://github.com/ydb-platform/ydb-embedded-ui/issues/1788)) ([1095d96](https://github.com/ydb-platform/ydb-embedded-ui/commit/1095d96fba7b803f013761ad250808e5f547566d))
* **Issues:** click on issue sets cursor to error in editor ([#1797](https://github.com/ydb-platform/ydb-embedded-ui/issues/1797)) ([6434633](https://github.com/ydb-platform/ydb-embedded-ui/commit/643463385501010fba0c901cc6fef6ab3b479b59))
* set pdisks column width ([#1793](https://github.com/ydb-platform/ydb-embedded-ui/issues/1793)) ([db11791](https://github.com/ydb-platform/ydb-embedded-ui/commit/db11791071f0b8721d3329516515ca92f5185f71))


### Bug Fixes

* **Auth:** do not show access error when redirecting to auth ([#1803](https://github.com/ydb-platform/ydb-embedded-ui/issues/1803)) ([b2dbe17](https://github.com/ydb-platform/ydb-embedded-ui/commit/b2dbe176f554dfda378b4ab4fd3acec7ac60a78d))
* **Cluster:** show loader if capabilities not loaded ([#1785](https://github.com/ydb-platform/ydb-embedded-ui/issues/1785)) ([01b8424](https://github.com/ydb-platform/ydb-embedded-ui/commit/01b8424a8040c1d422f7fc1ec87e92d3a833ad4e))
* handlers list that should be refetched in autorefresh mode ([#1791](https://github.com/ydb-platform/ydb-embedded-ui/issues/1791)) ([9d2280e](https://github.com/ydb-platform/ydb-embedded-ui/commit/9d2280e41fe5153fb5d5c62d30fc8ed25834c875))
* **VDiskPage:** fix evict action ([#1817](https://github.com/ydb-platform/ydb-embedded-ui/issues/1817)) ([3312ae2](https://github.com/ydb-platform/ydb-embedded-ui/commit/3312ae20bc0f912a4a2b28572e1833efdb770a73))

## [8.3.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v8.2.2...v8.3.0) (2024-12-23)


### Features

* display view query text ([#1780](https://github.com/ydb-platform/ydb-embedded-ui/issues/1780)) ([d590f05](https://github.com/ydb-platform/ydb-embedded-ui/commit/d590f0557c161dbed49a12fa419f307f6fa9f416))

## [8.2.2](https://github.com/ydb-platform/ydb-embedded-ui/compare/v8.2.1...v8.2.2) (2024-12-23)


### Bug Fixes

* **Table:** selectors specifity ([#1778](https://github.com/ydb-platform/ydb-embedded-ui/issues/1778)) ([005c672](https://github.com/ydb-platform/ydb-embedded-ui/commit/005c672a1319140dfa1dadab2bf3dd25ab99fc3b))

## [8.2.1](https://github.com/ydb-platform/ydb-embedded-ui/compare/v8.2.0...v8.2.1) (2024-12-23)


### Bug Fixes

* **Table:** increase styles specifity ([#1776](https://github.com/ydb-platform/ydb-embedded-ui/issues/1776)) ([96b8d2f](https://github.com/ydb-platform/ydb-embedded-ui/commit/96b8d2fc20bb9382cbb48a3aed87417ca1be5f39))

## [8.2.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v8.1.0...v8.2.0) (2024-12-20)


### Features

* add absolute timestamp to uptime ([#1768](https://github.com/ydb-platform/ydb-embedded-ui/issues/1768)) ([a77e6db](https://github.com/ydb-platform/ydb-embedded-ui/commit/a77e6db725c09e765529deb7b7b06f3f0d688e0a))
* vdisks in 2 rows ([#1758](https://github.com/ydb-platform/ydb-embedded-ui/issues/1758)) ([1f8d6fc](https://github.com/ydb-platform/ydb-embedded-ui/commit/1f8d6fc42612f39fb14b7de24f043e1554f00593))


### Bug Fixes

* create view template text ([#1772](https://github.com/ydb-platform/ydb-embedded-ui/issues/1772)) ([4773ce8](https://github.com/ydb-platform/ydb-embedded-ui/commit/4773ce82c48ae3a4940911475f38a355d83cd25b))
* **Schema:** increase default columns width ([#1765](https://github.com/ydb-platform/ydb-embedded-ui/issues/1765)) ([6be903d](https://github.com/ydb-platform/ydb-embedded-ui/commit/6be903d7dedbd9965994cb13ffbeed778d88dddf))
* **SchemaTree:** snippet not insert if user is not on Query tab ([#1766](https://github.com/ydb-platform/ydb-embedded-ui/issues/1766)) ([1e29666](https://github.com/ydb-platform/ydb-embedded-ui/commit/1e296666c6d3f38819f89784bb5e489124f90afe))
* **Stack:** reduce offset y on hover ([#1771](https://github.com/ydb-platform/ydb-embedded-ui/issues/1771)) ([640dd97](https://github.com/ydb-platform/ydb-embedded-ui/commit/640dd971f98803726eb51fad59866475c59abb37))
* **Table:** cell aligning should be maximum specific ([#1764](https://github.com/ydb-platform/ydb-embedded-ui/issues/1764)) ([5db78c9](https://github.com/ydb-platform/ydb-embedded-ui/commit/5db78c98be3c5c26b8ce8346aacc1f6ac78b0000))

## [8.1.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v8.0.0...v8.1.0) (2024-12-17)


### Features

* **autocomplete:** suggest variables ([#1752](https://github.com/ydb-platform/ydb-embedded-ui/issues/1752)) ([44e079a](https://github.com/ydb-platform/ydb-embedded-ui/commit/44e079a2001b40a20583311dcba621ea142e027c))
* remove /meta/cluster ([#1757](https://github.com/ydb-platform/ydb-embedded-ui/issues/1757)) ([6abc16e](https://github.com/ydb-platform/ydb-embedded-ui/commit/6abc16e5d31024884628c56d32704cb8daa354d7))


### Bug Fixes

* fix 1 week uptime formatting ([#1759](https://github.com/ydb-platform/ydb-embedded-ui/issues/1759)) ([e3de008](https://github.com/ydb-platform/ydb-embedded-ui/commit/e3de0084cbb6f1338dd892c74f0aaaaed1736104))

## [8.0.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v7.2.0...v8.0.0) (2024-12-12)


### ⚠ BREAKING CHANGES

* only pagination tables in storage ([#1745](https://github.com/ydb-platform/ydb-embedded-ui/issues/1745))

### Features

* change uptime format ([#1748](https://github.com/ydb-platform/ydb-embedded-ui/issues/1748)) ([d40aea0](https://github.com/ydb-platform/ydb-embedded-ui/commit/d40aea027dd87defdf2fbceb60eb44a5d480034d))
* remove unused exports, functions and files ([#1750](https://github.com/ydb-platform/ydb-embedded-ui/issues/1750)) ([cdf9ebc](https://github.com/ydb-platform/ydb-embedded-ui/commit/cdf9ebc46850cf5a0d150436bd4b07183b2080a8))


### Bug Fixes

* **ClusterInfo:** update links view ([#1746](https://github.com/ydb-platform/ydb-embedded-ui/issues/1746)) ([b3d5897](https://github.com/ydb-platform/ydb-embedded-ui/commit/b3d5897a05f503226458f4a47fcee113ced428db))
* execution plan svg not saving in chrome ([#1744](https://github.com/ydb-platform/ydb-embedded-ui/issues/1744)) ([189174a](https://github.com/ydb-platform/ydb-embedded-ui/commit/189174a20c8d8a1270e0f5fdaa86a5c43bfe0751))


### Miscellaneous Chores

* only pagination tables in storage ([#1745](https://github.com/ydb-platform/ydb-embedded-ui/issues/1745)) ([b12599e](https://github.com/ydb-platform/ydb-embedded-ui/commit/b12599e365d9ae9e0cbe3e87091568c575a0074e))

## [7.2.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v7.1.0...v7.2.0) (2024-12-10)


### Features

* add wrapper for gravity-ui/table ([#1736](https://github.com/ydb-platform/ydb-embedded-ui/issues/1736)) ([26d00e9](https://github.com/ydb-platform/ydb-embedded-ui/commit/26d00e910a078ad7042a3a41599978c0fb1995ad))
* **JSONTree:** allow case insensitive search ([#1735](https://github.com/ydb-platform/ydb-embedded-ui/issues/1735)) ([d4845d3](https://github.com/ydb-platform/ydb-embedded-ui/commit/d4845d3cd598ebbaece4934e7b0a4a886f657170))
* redirect to embedded ([#1732](https://github.com/ydb-platform/ydb-embedded-ui/issues/1732)) ([dacc546](https://github.com/ydb-platform/ydb-embedded-ui/commit/dacc546dc86f1e442c81dcd3120b40707d5a0504))
* show negative uptime for nodes when disconnected ([#1740](https://github.com/ydb-platform/ydb-embedded-ui/issues/1740)) ([3991c56](https://github.com/ydb-platform/ydb-embedded-ui/commit/3991c5666c222688fbcc24c578d58833ba258904))
* **TopQueries:** add limit, sort on backend ([#1737](https://github.com/ydb-platform/ydb-embedded-ui/issues/1737)) ([bc8acee](https://github.com/ydb-platform/ydb-embedded-ui/commit/bc8aceee8ac50c85110139324ddbed279b863925))


### Bug Fixes

* make grey more grey ([#1738](https://github.com/ydb-platform/ydb-embedded-ui/issues/1738)) ([6d39f9d](https://github.com/ydb-platform/ydb-embedded-ui/commit/6d39f9d498d86e2ec9f6763758a2c379aec71518))

## [7.1.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v7.0.0...v7.1.0) (2024-12-04)


### Features

* **ClusterInfo:** add cores and logging links ([#1731](https://github.com/ydb-platform/ydb-embedded-ui/issues/1731)) ([f8acb2b](https://github.com/ydb-platform/ydb-embedded-ui/commit/f8acb2baad8d92c06f620e5a7f03aba499c852f1))


### Bug Fixes

* **autocomplete:** should work properly if handler returns no entites ([#1726](https://github.com/ydb-platform/ydb-embedded-ui/issues/1726)) ([9cdab88](https://github.com/ydb-platform/ydb-embedded-ui/commit/9cdab88c00619e1c9d37faff27376d9156257fe1))

## [7.0.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v6.37.2...v7.0.0) (2024-12-02)


### ⚠ BREAKING CHANGES

* `window.api` was previously a flat `YdbEmbeddedAPI` object. It has been refactored into multiple distributed `AxiosWrapper` instances. Each endpoint is now handled by a dedicated wrapper instance, improving modularity and maintainability. Developers should update their integrations to use the new structure. For example: Replace `window.api.getTenants` with `window.api.viewer.getTenants`.

### Features

* **Network:** add peer role filter ([#1724](https://github.com/ydb-platform/ydb-embedded-ui/issues/1724)) ([1285049](https://github.com/ydb-platform/ydb-embedded-ui/commit/1285049d7101dcac432b9f9c934d58e7b074faa7))
* **Preview:** add rows count and truncated flag ([#1715](https://github.com/ydb-platform/ydb-embedded-ui/issues/1715)) ([6e1e701](https://github.com/ydb-platform/ydb-embedded-ui/commit/6e1e701b975b6fd3582bf7a6e30b2c97fb7d8235))
* **QueryResultTable:** display row number ([#1714](https://github.com/ydb-platform/ydb-embedded-ui/issues/1714)) ([eba72a0](https://github.com/ydb-platform/ydb-embedded-ui/commit/eba72a0a18605377f06e485f54d8072255900915))
* refactor API structure ([#1718](https://github.com/ydb-platform/ydb-embedded-ui/issues/1718)) ([e050bd7](https://github.com/ydb-platform/ydb-embedded-ui/commit/e050bd7e93355de1adf50b78c1c50dfcf78794e1))
* **Versions:** use columns from Nodes table ([#1713](https://github.com/ydb-platform/ydb-embedded-ui/issues/1713)) ([9b3f779](https://github.com/ydb-platform/ydb-embedded-ui/commit/9b3f779ce41579af1dc1420c32ab8d10d6ab7b7f))


### Bug Fixes

* dependabot found vulnerabilities ([#1720](https://github.com/ydb-platform/ydb-embedded-ui/issues/1720)) ([0faaf87](https://github.com/ydb-platform/ydb-embedded-ui/commit/0faaf87126728cc01a2961f901020ec07f2402ad))
* deploy test report error ([#1703](https://github.com/ydb-platform/ydb-embedded-ui/issues/1703)) ([efb0b9c](https://github.com/ydb-platform/ydb-embedded-ui/commit/efb0b9c65bb2b188f4aef8104c9c5ebf7e25143f))
* **FullScreen:** ensure the content is scrollable ([#1723](https://github.com/ydb-platform/ydb-embedded-ui/issues/1723)) ([f6e79f2](https://github.com/ydb-platform/ydb-embedded-ui/commit/f6e79f29fadf1b689dfc1e089cd122bde74e653a))
* **QueryResultTable:** optimise rendering ([#1697](https://github.com/ydb-platform/ydb-embedded-ui/issues/1697)) ([d93e866](https://github.com/ydb-platform/ydb-embedded-ui/commit/d93e866d8333cdf2fa0f3b3e2a16065f1cc4358a))
* **styles:** tune hover for tables ([#1710](https://github.com/ydb-platform/ydb-embedded-ui/issues/1710)) ([e86c845](https://github.com/ydb-platform/ydb-embedded-ui/commit/e86c8454ffc035786bdf7247ead77d80e4f3241f))
* **TopQueries:** add queryHashColumns ([#1701](https://github.com/ydb-platform/ydb-embedded-ui/issues/1701)) ([278f622](https://github.com/ydb-platform/ydb-embedded-ui/commit/278f622ad2886632bc6c0f42c366979ec7f4252f))
* **Versions:** tune default color ([#1719](https://github.com/ydb-platform/ydb-embedded-ui/issues/1719)) ([97c66b8](https://github.com/ydb-platform/ydb-embedded-ui/commit/97c66b87799695add99514f7173aedd1ef873077))

## [6.37.2](https://github.com/ydb-platform/ydb-embedded-ui/compare/v6.37.1...v6.37.2) (2024-11-26)


### Bug Fixes

* **AsideNavigation:** fix documentation link ([#1687](https://github.com/ydb-platform/ydb-embedded-ui/issues/1687)) ([017b983](https://github.com/ydb-platform/ydb-embedded-ui/commit/017b983ceed8c0899e6837f91f6c1178abd6fa6f))
* change default tables columns, reorder columns ([#1694](https://github.com/ydb-platform/ydb-embedded-ui/issues/1694)) ([f164489](https://github.com/ydb-platform/ydb-embedded-ui/commit/f1644891c4c59f47f15fc28c2071ec020885aca0))
* fallback for detailed memory ([#1696](https://github.com/ydb-platform/ydb-embedded-ui/issues/1696)) ([05094d7](https://github.com/ydb-platform/ydb-embedded-ui/commit/05094d7d7b657b9e9ee30ca5a8532aaa1cd82389))
* make limit rows less or equal to 100_000 ([#1695](https://github.com/ydb-platform/ydb-embedded-ui/issues/1695)) ([db8ec37](https://github.com/ydb-platform/ydb-embedded-ui/commit/db8ec37b43313617a00c762f44afedfd1a76f4e8))
* memory tests ([#1699](https://github.com/ydb-platform/ydb-embedded-ui/issues/1699)) ([4b1889f](https://github.com/ydb-platform/ydb-embedded-ui/commit/4b1889fa63aea2583850a6f6494eed360e7e56a0))

## [6.37.1](https://github.com/ydb-platform/ydb-embedded-ui/compare/v6.37.0...v6.37.1) (2024-11-25)


### Bug Fixes

* investigate 1 minute query timeout ([#1690](https://github.com/ydb-platform/ydb-embedded-ui/issues/1690)) ([70ac486](https://github.com/ydb-platform/ydb-embedded-ui/commit/70ac4869db5cf2930a201f0e639689381182e780))

## [6.37.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v6.36.1...v6.37.0) (2024-11-25)


### Features

* **autocomplete:** show column attributes ([#1665](https://github.com/ydb-platform/ydb-embedded-ui/issues/1665)) ([de0b9b6](https://github.com/ydb-platform/ydb-embedded-ui/commit/de0b9b6f1305e71b4f4a37e2b289f41971d131ea))
* indicate schema description loading ([#1672](https://github.com/ydb-platform/ydb-embedded-ui/issues/1672)) ([c0781f1](https://github.com/ydb-platform/ydb-embedded-ui/commit/c0781f102250e1aa6c7f4886919b08d97ce54e09))


### Bug Fixes

* add couple of template tests ([#1662](https://github.com/ydb-platform/ydb-embedded-ui/issues/1662)) ([80b3b88](https://github.com/ydb-platform/ydb-embedded-ui/commit/80b3b8889bdff30938bc62b5911433c40e5e785a))
* deploy report condition ([#1671](https://github.com/ydb-platform/ydb-embedded-ui/issues/1671)) ([09b6784](https://github.com/ydb-platform/ydb-embedded-ui/commit/09b678420c89d1bd76ef1c5b9bdeebf8579375aa))
* remove push trigger ([#1669](https://github.com/ydb-platform/ydb-embedded-ui/issues/1669)) ([108db81](https://github.com/ydb-platform/ydb-embedded-ui/commit/108db81c2e8115495ef946b181610ee5b5a2f637))
* show zero default for column ([#1681](https://github.com/ydb-platform/ydb-embedded-ui/issues/1681)) ([e33b702](https://github.com/ydb-platform/ydb-embedded-ui/commit/e33b7025ab967ef1c43bd21befeea170e18dacc2))
* split deploy and update pr ([#1670](https://github.com/ydb-platform/ydb-embedded-ui/issues/1670)) ([aa216b1](https://github.com/ydb-platform/ydb-embedded-ui/commit/aa216b11616a283786300a17805c33c9d0b3a2a2))

## [6.36.1](https://github.com/ydb-platform/ydb-embedded-ui/compare/v6.36.0...v6.36.1) (2024-11-21)


### Bug Fixes

* should not set filters for SELECT with known context ([#1666](https://github.com/ydb-platform/ydb-embedded-ui/issues/1666)) ([3e4ceef](https://github.com/ydb-platform/ydb-embedded-ui/commit/3e4ceef952a0821c94ec100f116fb97d033cc1f2))

## [6.36.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v6.35.0...v6.36.0) (2024-11-20)


### Features

* render tenant memory details ([#1623](https://github.com/ydb-platform/ydb-embedded-ui/issues/1623)) ([09b1238](https://github.com/ydb-platform/ydb-embedded-ui/commit/09b1238dfd6024778d187dcca2a5132d5c601fec))


### Bug Fixes

* make plan to svg more comprehensive ([#1658](https://github.com/ydb-platform/ydb-embedded-ui/issues/1658)) ([e973bae](https://github.com/ydb-platform/ydb-embedded-ui/commit/e973bae006fb67452a219f4f20f1772d8d73674f))
* **PaginatedTable:** fix autorefresh when no data ([#1650](https://github.com/ydb-platform/ydb-embedded-ui/issues/1650)) ([ed9a03b](https://github.com/ydb-platform/ydb-embedded-ui/commit/ed9a03b599d4fd8aaafab0c5e2005b2f111af009))
* **Versions:** should not show nodes withot version ([#1653](https://github.com/ydb-platform/ydb-embedded-ui/issues/1653)) ([fd60b9d](https://github.com/ydb-platform/ydb-embedded-ui/commit/fd60b9d0d71ecd98dfff2d254425d254525d6853))

## [6.35.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v6.34.0...v6.35.0) (2024-11-18)


### Features

* **Network:** add table view ([#1622](https://github.com/ydb-platform/ydb-embedded-ui/issues/1622)) ([507aa71](https://github.com/ydb-platform/ydb-embedded-ui/commit/507aa712afd529a6ea7e180e91c827d611844cfd))
* use monaco snippets for query templates ([#1626](https://github.com/ydb-platform/ydb-embedded-ui/issues/1626)) ([bcdefd5](https://github.com/ydb-platform/ydb-embedded-ui/commit/bcdefd5994161318c9da4b7a75beeac03c1bd14c))


### Bug Fixes

* move @ebay/nice-modal-react to dependencies ([#1647](https://github.com/ydb-platform/ydb-embedded-ui/issues/1647)) ([8887b75](https://github.com/ydb-platform/ydb-embedded-ui/commit/8887b7554dd65a5caf064ad84ebbc13af9a13f9a))
* remove empty selectedConsumer from url ([#1644](https://github.com/ydb-platform/ydb-embedded-ui/issues/1644)) ([14f3fa4](https://github.com/ydb-platform/ydb-embedded-ui/commit/14f3fa46747e9222bf93b82af19ec8b2dae853e4))
* **SaveQueryDialog:** should not duplicate component in DOM ([#1649](https://github.com/ydb-platform/ydb-embedded-ui/issues/1649)) ([3b37565](https://github.com/ydb-platform/ydb-embedded-ui/commit/3b37565241126763269aa8cc1e328e6ea2a14bc7))
* tests for templates ([#1648](https://github.com/ydb-platform/ydb-embedded-ui/issues/1648)) ([da2a02d](https://github.com/ydb-platform/ydb-embedded-ui/commit/da2a02d017b5af972fa68d5f5b02a4956e0763ab))

## [6.34.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v6.33.0...v6.34.0) (2024-11-18)


### Features

* add to embedded ui link to plan2svg converter ([#1619](https://github.com/ydb-platform/ydb-embedded-ui/issues/1619)) ([fb007a7](https://github.com/ydb-platform/ydb-embedded-ui/commit/fb007a764c8cf40d97242d598e4fa90310c97460))
* **ConfirmationDialog:** add saving query before replace ([#1629](https://github.com/ydb-platform/ydb-embedded-ui/issues/1629)) ([c71925d](https://github.com/ydb-platform/ydb-embedded-ui/commit/c71925dee41c588d5ca18f6170a62a15e32d9e8d))
* **Navigation:** allow to change user icon ([#1628](https://github.com/ydb-platform/ydb-embedded-ui/issues/1628)) ([71956ec](https://github.com/ydb-platform/ydb-embedded-ui/commit/71956ec3a1be3318f70b82c75656940a81478b3a))
* render per component memory consumption ([#1574](https://github.com/ydb-platform/ydb-embedded-ui/issues/1574)) ([3e4a04b](https://github.com/ydb-platform/ydb-embedded-ui/commit/3e4a04b2eaf68c98403b87f98ab1d31da930206a))
* warn about unsaved changes in editor ([#1620](https://github.com/ydb-platform/ydb-embedded-ui/issues/1620)) ([2632b90](https://github.com/ydb-platform/ydb-embedded-ui/commit/2632b90ed38cd89a1691b77b8cf4bccb2cf19507))


### Bug Fixes

* change trace and svg mutations to lazy query ([#1640](https://github.com/ydb-platform/ydb-embedded-ui/issues/1640)) ([19d7f56](https://github.com/ydb-platform/ydb-embedded-ui/commit/19d7f5629786531781dec738310b3cee8d3fd83e))
* delete directory from gh-pages on pr close ([#1638](https://github.com/ydb-platform/ydb-embedded-ui/issues/1638)) ([d99e295](https://github.com/ydb-platform/ydb-embedded-ui/commit/d99e295ba5574f04eb42def2770cc59113669b46))
* dont remove previous tests reports ([#1630](https://github.com/ydb-platform/ydb-embedded-ui/issues/1630)) ([5302a94](https://github.com/ydb-platform/ydb-embedded-ui/commit/5302a94e871eb55050bc2d462d9629b61cb9f685))
* **EntityStatus:** show title for text part only ([#1608](https://github.com/ydb-platform/ydb-embedded-ui/issues/1608)) ([7e234a5](https://github.com/ydb-platform/ydb-embedded-ui/commit/7e234a5e7d62e1bb845632f5bef9ab30103fc49e))
* **schemaActions:** preserve query settings when insert snippet ([#1615](https://github.com/ydb-platform/ydb-embedded-ui/issues/1615)) ([2ec5ccd](https://github.com/ydb-platform/ydb-embedded-ui/commit/2ec5ccde77a26228b148e46b7857b8767e134e1f))
* **schemaActions:** use different sets for row and column tables ([#1627](https://github.com/ydb-platform/ydb-embedded-ui/issues/1627)) ([9972ac2](https://github.com/ydb-platform/ydb-embedded-ui/commit/9972ac2fafc027564b7bc1df163ccd97de346801))
* **schemaQueryTemplates:** doc link for topic creation ([#1616](https://github.com/ydb-platform/ydb-embedded-ui/issues/1616)) ([3e07ca5](https://github.com/ydb-platform/ydb-embedded-ui/commit/3e07ca5488577aa7f2bad2731ae7ef09f8c66e6b))

## [6.33.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v6.32.0...v6.33.0) (2024-11-11)


### Features

* **autocomplete:** suggest all columns in a row ([#1599](https://github.com/ydb-platform/ydb-embedded-ui/issues/1599)) ([b4d2b7f](https://github.com/ydb-platform/ydb-embedded-ui/commit/b4d2b7f45c35b7eaadf93faf602cca16776093eb))


### Bug Fixes

* **Clusters:** controls and aggregations layout ([#1588](https://github.com/ydb-platform/ydb-embedded-ui/issues/1588)) ([ee61273](https://github.com/ydb-platform/ydb-embedded-ui/commit/ee61273c3b67a383a9c0ad8f7b783716cf53f750))
* **Nodes:** remove redundant group by params ([#1598](https://github.com/ydb-platform/ydb-embedded-ui/issues/1598)) ([d3c5714](https://github.com/ydb-platform/ydb-embedded-ui/commit/d3c571446d5b37dc83e359254d7872c01eca3c31))

## [6.32.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v6.31.0...v6.32.0) (2024-11-08)


### Features

* **Nodes:** add grouping ([#1584](https://github.com/ydb-platform/ydb-embedded-ui/issues/1584)) ([88ec214](https://github.com/ydb-platform/ydb-embedded-ui/commit/88ec214d3f89067a9c55243068c73a5d029a7068))


### Bug Fixes

* **Header:** fix developer ui link for embedded UI with proxy ([#1580](https://github.com/ydb-platform/ydb-embedded-ui/issues/1580)) ([b229301](https://github.com/ydb-platform/ydb-embedded-ui/commit/b229301e167c8651550606ee5450f6d7f2409048))
* if popover has actions increase delayClosing ([#1573](https://github.com/ydb-platform/ydb-embedded-ui/issues/1573)) ([7680e96](https://github.com/ydb-platform/ydb-embedded-ui/commit/7680e96681a9edf6b29589b73189ac72ef48f3d4))
* **NodeEndpointsTooltipContent:** change fields order ([#1585](https://github.com/ydb-platform/ydb-embedded-ui/issues/1585)) ([949a518](https://github.com/ydb-platform/ydb-embedded-ui/commit/949a518a272e920e36f4c2a94c3b502d37d6e3a4))
* query templates modification ([#1579](https://github.com/ydb-platform/ydb-embedded-ui/issues/1579)) ([697921a](https://github.com/ydb-platform/ydb-embedded-ui/commit/697921abe30e0b3415c0cd08519b8fecdfaaa03b))
* refresh schema and autoresresh icons should be similar ([#1572](https://github.com/ydb-platform/ydb-embedded-ui/issues/1572)) ([03bc63d](https://github.com/ydb-platform/ydb-embedded-ui/commit/03bc63dc5d386591a4ef2e73c7051349b37c83ed))
* use both BSC and Whiteboard for disks ([#1564](https://github.com/ydb-platform/ydb-embedded-ui/issues/1564)) ([da6dfcd](https://github.com/ydb-platform/ydb-embedded-ui/commit/da6dfcd856468ff89b031a2d6bbf710c7033ef5a))

## [6.31.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v6.30.0...v6.31.0) (2024-11-02)


### Features

* add control to execute forget command for an export ([#1552](https://github.com/ydb-platform/ydb-embedded-ui/issues/1552)) ([b31d2cf](https://github.com/ydb-platform/ydb-embedded-ui/commit/b31d2cfd44f34e0fc5a94d019389c4ca8ae77486))
* implement simple and narrow vertical progress bar ([#1560](https://github.com/ydb-platform/ydb-embedded-ui/issues/1560)) ([e5d0823](https://github.com/ydb-platform/ydb-embedded-ui/commit/e5d0823d7869d547f9c92d3681e8abc5c70000b9))
* **NodeEndpointsTooltipContent:** add fields ([#1566](https://github.com/ydb-platform/ydb-embedded-ui/issues/1566)) ([3287f99](https://github.com/ydb-platform/ydb-embedded-ui/commit/3287f9921ef1a92942c6fee4205e6a26465877d6))


### Bug Fixes

* **Nodes,Storage:** reorder controls - prevent moving on count change ([#1562](https://github.com/ydb-platform/ydb-embedded-ui/issues/1562)) ([74b27f7](https://github.com/ydb-platform/ydb-embedded-ui/commit/74b27f7758287dfc6861b9de032b24a4288aea63))
* **VDiskInfo:** fix title layout ([#1561](https://github.com/ydb-platform/ydb-embedded-ui/issues/1561)) ([7bd2262](https://github.com/ydb-platform/ydb-embedded-ui/commit/7bd2262ba583f7d007929d2d2d5c3ed9a1829a85))

## [6.30.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v6.29.3...v6.30.0) (2024-10-31)


### Features

* **ObjectSummary:** add button to refresh tree ([#1559](https://github.com/ydb-platform/ydb-embedded-ui/issues/1559)) ([e7dbf9d](https://github.com/ydb-platform/ydb-embedded-ui/commit/e7dbf9dcabfa6b0f8fed0e16886e59e1c9b46d09))


### Bug Fixes

* **Cluster:** use /capabilities to show dashboard ([#1556](https://github.com/ydb-platform/ydb-embedded-ui/issues/1556)) ([7811347](https://github.com/ydb-platform/ydb-embedded-ui/commit/78113477d3a9b0bee26de20b4a4a9e38e8eefd5f))
* pass database to capabilities query ([#1551](https://github.com/ydb-platform/ydb-embedded-ui/issues/1551)) ([7e7b3e3](https://github.com/ydb-platform/ydb-embedded-ui/commit/7e7b3e38c82aa7df8a454f6ce2bc4f37c2fd1a99))
* tracing issues ([#1555](https://github.com/ydb-platform/ydb-embedded-ui/issues/1555)) ([4cb2d7f](https://github.com/ydb-platform/ydb-embedded-ui/commit/4cb2d7fdd59126a44ef3b003a73e3375ba1f853b))

## [6.29.3](https://github.com/ydb-platform/ydb-embedded-ui/compare/v6.29.2...v6.29.3) (2024-10-28)


### Bug Fixes

* very bad performance when scrolling paginated tables ([#1513](https://github.com/ydb-platform/ydb-embedded-ui/issues/1513)) ([e2f7a25](https://github.com/ydb-platform/ydb-embedded-ui/commit/e2f7a2557e4699f2737b7f660579fec47580f48a))

## [6.29.2](https://github.com/ydb-platform/ydb-embedded-ui/compare/v6.29.1...v6.29.2) (2024-10-28)


### Bug Fixes

* **ClusterMetricsCores:** format value and capacity same way ([#1532](https://github.com/ydb-platform/ydb-embedded-ui/issues/1532)) ([4eb5c04](https://github.com/ydb-platform/ydb-embedded-ui/commit/4eb5c041f8b5615a2806bf5d5bd015afa963ac09))
* **TabletsTable:** action icons coincide in tablet's page and in table ([#1549](https://github.com/ydb-platform/ydb-embedded-ui/issues/1549)) ([26f77a8](https://github.com/ydb-platform/ydb-embedded-ui/commit/26f77a8563e3c3a100001b198e3e7ee5f97ad7f9))

## [6.29.1](https://github.com/ydb-platform/ydb-embedded-ui/compare/v6.29.0...v6.29.1) (2024-10-25)


### Bug Fixes

* **EntityStatus:** wrapper layout without clipboard ([#1538](https://github.com/ydb-platform/ydb-embedded-ui/issues/1538)) ([9858369](https://github.com/ydb-platform/ydb-embedded-ui/commit/9858369022c5921f1c66b17341355d2d97dd2e66))
* primary keys for column tables ([#1541](https://github.com/ydb-platform/ydb-embedded-ui/issues/1541)) ([4359ca6](https://github.com/ydb-platform/ydb-embedded-ui/commit/4359ca6124da4e59e9541c6534bf524f3038c3ec))

## [6.29.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v6.28.0...v6.29.0) (2024-10-25)


### Features

* add Bytes and Text types suggestion in autocomplete ([#1531](https://github.com/ydb-platform/ydb-embedded-ui/issues/1531)) ([6a99452](https://github.com/ydb-platform/ydb-embedded-ui/commit/6a994523986f68d775c1efb13f49c624f0125107))


### Bug Fixes

* **ClusterDashboard:** hide dashboard if /cluster handler version less 5 ([#1535](https://github.com/ydb-platform/ydb-embedded-ui/issues/1535)) ([f24e5e4](https://github.com/ydb-platform/ydb-embedded-ui/commit/f24e5e4cb2498fd1fa828d8dc40a7f36253016a1))
* remove excessive slash for requests via tenant node ([#1537](https://github.com/ydb-platform/ydb-embedded-ui/issues/1537)) ([6fb97a9](https://github.com/ydb-platform/ydb-embedded-ui/commit/6fb97a96710bccc39918bb5b127df421df1cbfc4))
* **TenantOverview:** fix used tablet storage calculation ([#1528](https://github.com/ydb-platform/ydb-embedded-ui/issues/1528)) ([96411ee](https://github.com/ydb-platform/ydb-embedded-ui/commit/96411ee7b3f7f1830c2bd445c584438964cc7347))

## [6.28.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v6.27.1...v6.28.0) (2024-10-23)


### Features

* add developer ui links to disks popups ([#1512](https://github.com/ydb-platform/ydb-embedded-ui/issues/1512)) ([57d2092](https://github.com/ydb-platform/ydb-embedded-ui/commit/57d20927839b4a11b7f24b57f7d3cf9b7549fb0f))


### Bug Fixes

* **Cluster:** handle cluster error in meta requests ([#1525](https://github.com/ydb-platform/ydb-embedded-ui/issues/1525)) ([71b254e](https://github.com/ydb-platform/ydb-embedded-ui/commit/71b254ec73ae492d722f8a720cda4e1de714e286))
* correct width for columns with ProgressViewer ([#1522](https://github.com/ydb-platform/ydb-embedded-ui/issues/1522)) ([e27749a](https://github.com/ydb-platform/ydb-embedded-ui/commit/e27749af04d9230edd8cd24f5e3a59d968d87dcf))
* **EntityStatus:** remove additionalControls, fix ClipboardButton layout ([#1524](https://github.com/ydb-platform/ydb-embedded-ui/issues/1524)) ([ea4d3a9](https://github.com/ydb-platform/ydb-embedded-ui/commit/ea4d3a95ffe99c8380f0a8971ff41f9b9b534aa7))
* fields required in all groups and nodes requests ([#1515](https://github.com/ydb-platform/ydb-embedded-ui/issues/1515)) ([a9f79a3](https://github.com/ydb-platform/ydb-embedded-ui/commit/a9f79a31ced8a642dabd6231df4e810e1f14eaa3))
* operations kind button width ([#1521](https://github.com/ydb-platform/ydb-embedded-ui/issues/1521)) ([980d4fa](https://github.com/ydb-platform/ydb-embedded-ui/commit/980d4facf842eb2f64275afaf925e6dde309c0fe))
* replace EntityStatus with StatusIcon where possible ([#1518](https://github.com/ydb-platform/ydb-embedded-ui/issues/1518)) ([017c82a](https://github.com/ydb-platform/ydb-embedded-ui/commit/017c82a54fac97c2e62422ac971f615f39434d84))
* **Storage:** display unavailable vdisks with average size ([#1511](https://github.com/ydb-platform/ydb-embedded-ui/issues/1511)) ([1868e18](https://github.com/ydb-platform/ydb-embedded-ui/commit/1868e182bd88fec178b400f32219f80f1cee1c02))
* update license ([#1516](https://github.com/ydb-platform/ydb-embedded-ui/issues/1516)) ([cabcb5c](https://github.com/ydb-platform/ydb-embedded-ui/commit/cabcb5c89b0fa50ea4fdb2018eff39091d1b440f))

## [6.27.1](https://github.com/ydb-platform/ydb-embedded-ui/compare/v6.27.0...v6.27.1) (2024-10-22)


### Bug Fixes

* increase storage/groups timeout ([#1510](https://github.com/ydb-platform/ydb-embedded-ui/issues/1510)) ([b3dad05](https://github.com/ydb-platform/ydb-embedded-ui/commit/b3dad0545744ba091349c468ec5b62408dcfaff5))
* remove unneeded titles ([#1508](https://github.com/ydb-platform/ydb-embedded-ui/issues/1508)) ([b4c44e9](https://github.com/ydb-platform/ydb-embedded-ui/commit/b4c44e9f668bbf3528bc156117e821d126225865))

## [6.27.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v6.26.1...v6.27.0) (2024-10-21)


### Features

* **Cluster:** rework cluster page ([#1473](https://github.com/ydb-platform/ydb-embedded-ui/issues/1473)) ([ee06b4e](https://github.com/ydb-platform/ydb-embedded-ui/commit/ee06b4e47e0450d7c1e8e2ba79d00d6786155425))
* display a list of operations ([#1445](https://github.com/ydb-platform/ydb-embedded-ui/issues/1445)) ([3dda3fe](https://github.com/ydb-platform/ydb-embedded-ui/commit/3dda3feab9ba43c0ff6b73ce6a6d7c87ed32de0e))
* **Storage,Nodes:** request only needed fields from backend ([#1491](https://github.com/ydb-platform/ydb-embedded-ui/issues/1491)) ([0af72a4](https://github.com/ydb-platform/ydb-embedded-ui/commit/0af72a42c7035aef519340334a7c954e86ab272d))


### Bug Fixes

* cluster layout ([#1507](https://github.com/ydb-platform/ydb-embedded-ui/issues/1507)) ([d38d01b](https://github.com/ydb-platform/ydb-embedded-ui/commit/d38d01b9a1dfd1c8b5ccb6cf814a5d02689e3f42))
* fix cdc stream query template ([#1498](https://github.com/ydb-platform/ydb-embedded-ui/issues/1498)) ([8405466](https://github.com/ydb-platform/ydb-embedded-ui/commit/84054666dae4df611a9657027b7be2bde84f153d))
* **overview:** broken loading state calculation ([#1499](https://github.com/ydb-platform/ydb-embedded-ui/issues/1499)) ([05d89be](https://github.com/ydb-platform/ydb-embedded-ui/commit/05d89be1bcc8718781ec003b4444a922c32b0e43))
* **Preview:** do not auto refresh table preview ([#1503](https://github.com/ydb-platform/ydb-embedded-ui/issues/1503)) ([dbe83b2](https://github.com/ydb-platform/ydb-embedded-ui/commit/dbe83b2ee05cf5b2b01c876b59f8d7a127ac0f67))
* **topic:** broken memoization ([#1500](https://github.com/ydb-platform/ydb-embedded-ui/issues/1500)) ([e85f70e](https://github.com/ydb-platform/ydb-embedded-ui/commit/e85f70ea2f6053d3b9dae7e0f6630b3fe672a488))

## [6.26.1](https://github.com/ydb-platform/ydb-embedded-ui/compare/v6.26.0...v6.26.1) (2024-10-18)


### Bug Fixes

* **EntityStatus:** set minimum container width ([#1494](https://github.com/ydb-platform/ydb-embedded-ui/issues/1494)) ([d6c6a4c](https://github.com/ydb-platform/ydb-embedded-ui/commit/d6c6a4c709e5b8ce6bdff85fe756b1299805b776))

## [6.26.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v6.25.0...v6.26.0) (2024-10-18)


### Features

* add/drop table index template ([#1456](https://github.com/ydb-platform/ydb-embedded-ui/issues/1456)) ([d7a8e0b](https://github.com/ydb-platform/ydb-embedded-ui/commit/d7a8e0ba5d59ac0d9f483b8f70083393531b8a5c))
* improve appearing controls styles ([#1436](https://github.com/ydb-platform/ydb-embedded-ui/issues/1436)) ([830c0a5](https://github.com/ydb-platform/ydb-embedded-ui/commit/830c0a503db6a49b2daf316585e0f31d998149d1))
* **ObjectSummary:** improve object overview ([#1447](https://github.com/ydb-platform/ydb-embedded-ui/issues/1447)) ([4afa2b6](https://github.com/ydb-platform/ydb-embedded-ui/commit/4afa2b65370772a78980114f856579e8c73b5952))
* paginated tables - enable setting by default ([#1464](https://github.com/ydb-platform/ydb-embedded-ui/issues/1464)) ([9006a52](https://github.com/ydb-platform/ydb-embedded-ui/commit/9006a527be9c75227cd970eda848bf0316977616))
* **RunningQueries:** add userSID search ([#1462](https://github.com/ydb-platform/ydb-embedded-ui/issues/1462)) ([1194b34](https://github.com/ydb-platform/ydb-embedded-ui/commit/1194b34cacacc25c7547da60e6f6ec305bfae173))
* snippets for table (under tree dots in navigation tree) ([#1476](https://github.com/ydb-platform/ydb-embedded-ui/issues/1476)) ([39d86c9](https://github.com/ydb-platform/ydb-embedded-ui/commit/39d86c9a9492acf6d7f8b82e8713c2b412a952b8))
* **Versions:** show overall version info in Versions tab ([#1442](https://github.com/ydb-platform/ydb-embedded-ui/issues/1442)) ([6cc07d5](https://github.com/ydb-platform/ydb-embedded-ui/commit/6cc07d5e55d1ac9d601ea9e463085fc241465988))


### Bug Fixes

* **Cluster, TabletsTable:** add node fqdn and loading state ([#1468](https://github.com/ydb-platform/ydb-embedded-ui/issues/1468)) ([4090696](https://github.com/ydb-platform/ydb-embedded-ui/commit/40906964c978189c84dfff86c9c581756d5ebfa5))
* correct key columns order ([#1478](https://github.com/ydb-platform/ydb-embedded-ui/issues/1478)) ([da4cf1f](https://github.com/ydb-platform/ydb-embedded-ui/commit/da4cf1ffd25b7fbba374d933099be01785c6b49f))
* exclude top and running queries itself ([#1487](https://github.com/ydb-platform/ydb-embedded-ui/issues/1487)) ([4c91b29](https://github.com/ydb-platform/ydb-embedded-ui/commit/4c91b2928c5a3504a47bbd82f65120aaafcf18c1))
* get info about topic children from overview ([#1489](https://github.com/ydb-platform/ydb-embedded-ui/issues/1489)) ([82531a5](https://github.com/ydb-platform/ydb-embedded-ui/commit/82531a584e59ff8a6c17343e3ec18772b9e39c05))
* popup closes on context menu copy ([#1453](https://github.com/ydb-platform/ydb-embedded-ui/issues/1453)) ([9daa5a3](https://github.com/ydb-platform/ydb-embedded-ui/commit/9daa5a384a433dac57bc5822a72df108067ef973))
* **Tablet:** remove nodeId from header and api requests for tablet ([#1461](https://github.com/ydb-platform/ydb-embedded-ui/issues/1461)) ([e452f15](https://github.com/ydb-platform/ydb-embedded-ui/commit/e452f15b797dc85ff5af3fffe43f7fa42ad7f5bf))
* turn off default paginated tables ([#1475](https://github.com/ydb-platform/ydb-embedded-ui/issues/1475)) ([d4f528c](https://github.com/ydb-platform/ydb-embedded-ui/commit/d4f528cafb660ddc603b1ba9f740f7d119526985))
* **Versions:** request only SystemState ([#1481](https://github.com/ydb-platform/ydb-embedded-ui/issues/1481)) ([df74377](https://github.com/ydb-platform/ydb-embedded-ui/commit/df743773c2f259264fef82a7fd13d354c640b478))

## [6.25.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v6.24.0...v6.25.0) (2024-10-11)


### Features

* **Cluster:** add Tablets tab ([#1438](https://github.com/ydb-platform/ydb-embedded-ui/issues/1438)) ([f349bcd](https://github.com/ydb-platform/ydb-embedded-ui/commit/f349bcdf758483e42aa571ba293cf3558a9d2e34))


### Bug Fixes

* do not hide pdisk and vdisk popups if mouse on popup content ([#1435](https://github.com/ydb-platform/ydb-embedded-ui/issues/1435)) ([ac70e8d](https://github.com/ydb-platform/ydb-embedded-ui/commit/ac70e8d62ec81913e0d4256900cdc4422fa44cf5))
* **ExecuteResult:** do not show title if no result ([#1444](https://github.com/ydb-platform/ydb-embedded-ui/issues/1444)) ([78cd713](https://github.com/ydb-platform/ydb-embedded-ui/commit/78cd7137f5f77990daaca28a460766baaa5f96b1))
* **Storage:** do not display group control if not available ([#1449](https://github.com/ydb-platform/ydb-embedded-ui/issues/1449)) ([a647026](https://github.com/ydb-platform/ydb-embedded-ui/commit/a6470263d3f9a2948be0baefcf13fbfb80b282ca))

## [6.24.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v6.23.1...v6.24.0) (2024-10-10)


### Features

* add dimming to vdisk page (nodes) and pdisk page (vdisks) ([#1397](https://github.com/ydb-platform/ydb-embedded-ui/issues/1397)) ([deb2a88](https://github.com/ydb-platform/ydb-embedded-ui/commit/deb2a88d86ac0ad84f13acf1b7a3bf89289f5722))
* **Databases:** use balancer + /node/:id as backend endpoint ([#1418](https://github.com/ydb-platform/ydb-embedded-ui/issues/1418)) ([f8a0db1](https://github.com/ydb-platform/ydb-embedded-ui/commit/f8a0db18fa01b9e9198e7c1ad4a656ba17de5922))
* **Storage:** add disk space usage column ([#1425](https://github.com/ydb-platform/ydb-embedded-ui/issues/1425)) ([d254ee2](https://github.com/ydb-platform/ydb-embedded-ui/commit/d254ee2bd71b5028eb48b4d34c30bc5ea48fa484))
* **StorageNodes:** add columns, use the same nodes columns ([#1396](https://github.com/ydb-platform/ydb-embedded-ui/issues/1396)) ([90a3403](https://github.com/ydb-platform/ydb-embedded-ui/commit/90a34037c8ea8e7fe8e680a47d6e1323b9fd9ff2))
* **VDisk:** show VDisk donors inside popup ([#1422](https://github.com/ydb-platform/ydb-embedded-ui/issues/1422)) ([fc12a38](https://github.com/ydb-platform/ydb-embedded-ui/commit/fc12a38156e117e43e89b301b58df59408a98928))


### Bug Fixes

* **Authentication:** handle login error properly ([#1426](https://github.com/ydb-platform/ydb-embedded-ui/issues/1426)) ([10f817e](https://github.com/ydb-platform/ydb-embedded-ui/commit/10f817ef9879ce8fc3beddda1377a03ca285bc5f))
* autocomplete not working in standalone version ([#1405](https://github.com/ydb-platform/ydb-embedded-ui/issues/1405)) ([8f516be](https://github.com/ydb-platform/ydb-embedded-ui/commit/8f516becf0fc0cfdd25d10abf00a41dc6953a326))
* **cluster:** infoV2 check ([#1401](https://github.com/ydb-platform/ydb-embedded-ui/issues/1401)) ([6dbe56d](https://github.com/ydb-platform/ydb-embedded-ui/commit/6dbe56db0e3a85c44852b85477e2027f8e1754b4))
* nodes list stops working on sort by uptime scroll down ([#1424](https://github.com/ydb-platform/ydb-embedded-ui/issues/1424)) ([5ecaecb](https://github.com/ydb-platform/ydb-embedded-ui/commit/5ecaecbfdedd47d39539e871cca8a7cf5a45e1f0))
* **StorageGroups:** display full pool name with left cut ([#1421](https://github.com/ydb-platform/ydb-embedded-ui/issues/1421)) ([09599c1](https://github.com/ydb-platform/ydb-embedded-ui/commit/09599c1d920fc691e2e8f97ced5ccf057651ca53))
* **StorageGroups:** fix latency column ([#1403](https://github.com/ydb-platform/ydb-embedded-ui/issues/1403)) ([5e1961c](https://github.com/ydb-platform/ydb-embedded-ui/commit/5e1961c72d2904be7a7700259c1b517c5fa997db))
* **Storage:** prevent duplicating vdisks when no whiteboard ([#1420](https://github.com/ydb-platform/ydb-embedded-ui/issues/1420)) ([73e9e6b](https://github.com/ydb-platform/ydb-embedded-ui/commit/73e9e6bf939873c497b900286f8557ba12621917))
* tracelevel none by default ([#1432](https://github.com/ydb-platform/ydb-embedded-ui/issues/1432)) ([1c786cb](https://github.com/ydb-platform/ydb-embedded-ui/commit/1c786cb2b62e2470b79193b6b2d10b46ed384ff6))
* use babel loader from create-react-app ([#1409](https://github.com/ydb-platform/ydb-embedded-ui/issues/1409)) ([0a4734e](https://github.com/ydb-platform/ydb-embedded-ui/commit/0a4734ebc54c036c422fd269e8e6b691af5a6b5d))

## [6.23.1](https://github.com/ydb-platform/ydb-embedded-ui/compare/v6.23.0...v6.23.1) (2024-10-03)


### Bug Fixes

* **Cluster:** contatiner should take its parent width ([#1393](https://github.com/ydb-platform/ydb-embedded-ui/issues/1393)) ([a34bb3e](https://github.com/ydb-platform/ydb-embedded-ui/commit/a34bb3e1248a4656887b25118e4d4b153aa485b7))

## [6.23.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v6.22.0...v6.23.0) (2024-10-02)


### Features

* dim vdisks on the node page that are not on the selected node ([#1369](https://github.com/ydb-platform/ydb-embedded-ui/issues/1369)) ([446019f](https://github.com/ydb-platform/ydb-embedded-ui/commit/446019f06ecd26c775dbf59fc9d5bed0b786f0ca))
* **ExecuteResult:** add row count for all results ([#1368](https://github.com/ydb-platform/ydb-embedded-ui/issues/1368)) ([570507e](https://github.com/ydb-platform/ydb-embedded-ui/commit/570507e911d339fc994df4d0d9ebf7c4e9f0ed94))
* move query templates to a hierarchical menu at query editor ([#1327](https://github.com/ydb-platform/ydb-embedded-ui/issues/1327)) ([960e97f](https://github.com/ydb-platform/ydb-embedded-ui/commit/960e97fcdf1e2d358006abd7811e92c1243f2109))
* **Nodes:** add node name column ([#1385](https://github.com/ydb-platform/ydb-embedded-ui/issues/1385)) ([a4c24e1](https://github.com/ydb-platform/ydb-embedded-ui/commit/a4c24e147b9e09c1675ee6a597b3c8e56f1602d7))
* **PaginatedStorage:** add grouping ([#1364](https://github.com/ydb-platform/ydb-embedded-ui/issues/1364)) ([93dd920](https://github.com/ydb-platform/ydb-embedded-ui/commit/93dd9200a0a299195f7887fa2b04c3b0e5c4d476))
* show running queries ([#1313](https://github.com/ydb-platform/ydb-embedded-ui/issues/1313)) ([acd4a1e](https://github.com/ydb-platform/ydb-embedded-ui/commit/acd4a1ec7e83806aef106168e843f196879d71e7))
* **StorageGroups:** add latency and allocation units columns ([#1390](https://github.com/ydb-platform/ydb-embedded-ui/issues/1390)) ([4cb6fed](https://github.com/ydb-platform/ydb-embedded-ui/commit/4cb6fedc30b969c0e5a38e18415145389e9dc86a))
* use relative entity  path in schema actions ([#1366](https://github.com/ydb-platform/ydb-embedded-ui/issues/1366)) ([418d3d8](https://github.com/ydb-platform/ydb-embedded-ui/commit/418d3d8a74256b4eee2e74084a11acc61e819f11))


### Bug Fixes

* always show required columns ([#1374](https://github.com/ydb-platform/ydb-embedded-ui/issues/1374)) ([6cab2df](https://github.com/ydb-platform/ydb-embedded-ui/commit/6cab2df320535cb0c4fdcd163608f677e2c603a6))
* **Diagnostics:** hide graph for column tables ([#1356](https://github.com/ydb-platform/ydb-embedded-ui/issues/1356)) ([4fe1a24](https://github.com/ydb-platform/ydb-embedded-ui/commit/4fe1a24a90e2bf2d86ef9c665ce6cb71d24519ea))
* different measurements look confusing ([#1381](https://github.com/ydb-platform/ydb-embedded-ui/issues/1381)) ([5f26767](https://github.com/ydb-platform/ydb-embedded-ui/commit/5f2676737a6f2733c30562f69b3f80fb06229a7e))
* fix columns width on long rowset ([#1384](https://github.com/ydb-platform/ydb-embedded-ui/issues/1384)) ([f30a38f](https://github.com/ydb-platform/ydb-embedded-ui/commit/f30a38f5801b3a3eef2ca8c44fa980718682e76e))
* **Header:** show cluster name in breadcrumb for PDisk/VDisk pages ([#1357](https://github.com/ydb-platform/ydb-embedded-ui/issues/1357)) ([56839bc](https://github.com/ydb-platform/ydb-embedded-ui/commit/56839bc12bccd3874f7270e2951711f8d3811068))
* **PaginatedStorage:** properly pass ids, display 1 node on Node page ([#1382](https://github.com/ydb-platform/ydb-embedded-ui/issues/1382)) ([e63d22b](https://github.com/ydb-platform/ydb-embedded-ui/commit/e63d22b41d85059f66d311b299b28a330097669a))
* **PDisk:** show node host name in popup ([#1352](https://github.com/ydb-platform/ydb-embedded-ui/issues/1352)) ([12010dd](https://github.com/ydb-platform/ydb-embedded-ui/commit/12010ddd75d90b1e3f30e55916e39481298858b4))
* **Storage:** completely remove usage filter ([#1375](https://github.com/ydb-platform/ydb-embedded-ui/issues/1375)) ([d200b4c](https://github.com/ydb-platform/ydb-embedded-ui/commit/d200b4c0d07060b946fb03d9332895f8b2583d41))
* **StorageNodes:** uniform render type ([#1376](https://github.com/ydb-platform/ydb-embedded-ui/issues/1376)) ([9e25733](https://github.com/ydb-platform/ydb-embedded-ui/commit/9e2573363f3e1c9aa54a5ed47d37821fffcdbc41))

## [6.22.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v6.21.0...v6.22.0) (2024-09-24)


### Features

* add rows limit to query settings ([#1291](https://github.com/ydb-platform/ydb-embedded-ui/issues/1291)) ([1728e61](https://github.com/ydb-platform/ydb-embedded-ui/commit/1728e6162e5be9c5fdc0954595b562873ad6fb9b))
* big query results freeze interface ([#1354](https://github.com/ydb-platform/ydb-embedded-ui/issues/1354)) ([660f89c](https://github.com/ydb-platform/ydb-embedded-ui/commit/660f89cd696f85b2ea61ce31a64e7ef2d70f9283))
* in query results show when it was truncated ([#1309](https://github.com/ydb-platform/ydb-embedded-ui/issues/1309)) ([0434410](https://github.com/ydb-platform/ydb-embedded-ui/commit/043441045d5e9cc463271da505bec6bf04489f5d))
* **Nodes:** add columns setup ([#1320](https://github.com/ydb-platform/ydb-embedded-ui/issues/1320)) ([c9ec3b7](https://github.com/ydb-platform/ydb-embedded-ui/commit/c9ec3b790d461ea150d315bb0f364e979218f788))
* **ObjectSummary:** add link to Schema in Diagnostics ([#1323](https://github.com/ydb-platform/ydb-embedded-ui/issues/1323)) ([deaf519](https://github.com/ydb-platform/ydb-embedded-ui/commit/deaf519c35b7df50919bf12de6fd87fe2a764aa9))
* **ObjectSummary:** add paths and shards limits ([#1326](https://github.com/ydb-platform/ydb-embedded-ui/issues/1326)) ([61c3562](https://github.com/ydb-platform/ydb-embedded-ui/commit/61c3562ea569b217d2fffb5622b38d2054240aeb))
* show vdisks of selected group in a special color ([#1336](https://github.com/ydb-platform/ydb-embedded-ui/issues/1336)) ([5402c86](https://github.com/ydb-platform/ydb-embedded-ui/commit/5402c86a9a07f8999bb084ac2d50ebda54c9adc6))
* **Storage:** add columns setup ([#1321](https://github.com/ydb-platform/ydb-embedded-ui/issues/1321)) ([b53487e](https://github.com/ydb-platform/ydb-embedded-ui/commit/b53487e7a6ad5779dbc02b08eaf4b1a9929021b6))


### Bug Fixes

* **Clusters:** fix columns setup do not save values ([#1316](https://github.com/ydb-platform/ydb-embedded-ui/issues/1316)) ([964122f](https://github.com/ydb-platform/ydb-embedded-ui/commit/964122f5560cff67fcb3562cfa7ee05f33646b5b))
* do not send empty strings in filters ([#1330](https://github.com/ydb-platform/ydb-embedded-ui/issues/1330)) ([5af61ac](https://github.com/ydb-platform/ydb-embedded-ui/commit/5af61acd52202c58815883bcfa179bfcbaabc8c3))
* show structure tab and PDisk/VDisk if disk's new API is absent ([#1338](https://github.com/ydb-platform/ydb-embedded-ui/issues/1338)) ([6213ed7](https://github.com/ydb-platform/ydb-embedded-ui/commit/6213ed7856a80cb2158bc0ff1af79ed160b3b3f1))
* **Storage:** fix additional props not passed ([#1350](https://github.com/ydb-platform/ydb-embedded-ui/issues/1350)) ([e8beb49](https://github.com/ydb-platform/ydb-embedded-ui/commit/e8beb4900685be5c7d7c339364d3941bd25fadda))
* **StorageGroups:** make group id first column ([#1351](https://github.com/ydb-platform/ydb-embedded-ui/issues/1351)) ([73642db](https://github.com/ydb-platform/ydb-embedded-ui/commit/73642db54728fb15be91de64e9ca0eb60ac2a857))

## [6.21.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v6.20.0...v6.21.0) (2024-09-18)


### Features

* add storage group page ([#1289](https://github.com/ydb-platform/ydb-embedded-ui/issues/1289)) ([8e05166](https://github.com/ydb-platform/ydb-embedded-ui/commit/8e05166388964c9fbe01984723c8d3e4cedd48a8))


### Bug Fixes

* **api:** correctly get cluster info from data (/meta/db_clusters) ([#1314](https://github.com/ydb-platform/ydb-embedded-ui/issues/1314)) ([762f8ed](https://github.com/ydb-platform/ydb-embedded-ui/commit/762f8ede6cda7b3f3e20eb7d69286d5ff76ea627))
* check feature flag api is available ([#1308](https://github.com/ydb-platform/ydb-embedded-ui/issues/1308)) ([8531b95](https://github.com/ydb-platform/ydb-embedded-ui/commit/8531b954cd8870c5ecf7495064a5bac65aea55d6))

## [6.20.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v6.19.0...v6.20.0) (2024-09-17)


### Features

* use new /viewer/cluster handler format ([#1272](https://github.com/ydb-platform/ydb-embedded-ui/issues/1272)) ([f0742ab](https://github.com/ydb-platform/ydb-embedded-ui/commit/f0742ab480bf595f7b5a250dcbd23d33f15ea523))


### Bug Fixes

* **DecommissionButton:** add checkbox to dialog ([#1294](https://github.com/ydb-platform/ydb-embedded-ui/issues/1294)) ([6d65839](https://github.com/ydb-platform/ydb-embedded-ui/commit/6d6583951e31d2ad407077dbd9c9b29472a4983a))
* **queries:** do not fail on response with 200 code and null body ([#1304](https://github.com/ydb-platform/ydb-embedded-ui/issues/1304)) ([d115989](https://github.com/ydb-platform/ydb-embedded-ui/commit/d115989a8ad0479a8269507df14c4b533f7d2084))
* remove break nodes setting, remove compute ([#1306](https://github.com/ydb-platform/ydb-embedded-ui/issues/1306)) ([de315ae](https://github.com/ydb-platform/ydb-embedded-ui/commit/de315ae980b3e7d4bbea48f1497934d11180002a))

## [6.19.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v6.18.0...v6.19.0) (2024-09-13)


### Features

* add storage/groups endpoint ([#1273](https://github.com/ydb-platform/ydb-embedded-ui/issues/1273)) ([42ee9a4](https://github.com/ydb-platform/ydb-embedded-ui/commit/42ee9a470632807650cfc0dd934f5b0a3d30c909))
* **Diagnostics:** display db feature flags ([#1229](https://github.com/ydb-platform/ydb-embedded-ui/issues/1229)) ([182d803](https://github.com/ydb-platform/ydb-embedded-ui/commit/182d803205667794f16811de1be5ea685b1478e8))
* **Header:** use cluster domain if cluster name is undefined ([#1271](https://github.com/ydb-platform/ydb-embedded-ui/issues/1271)) ([125d556](https://github.com/ydb-platform/ydb-embedded-ui/commit/125d556810a4013aee9a97bfde3db27f3e4e0b5b))
* **Node:** enable nodes switch in storage tab on node page ([#1277](https://github.com/ydb-platform/ydb-embedded-ui/issues/1277)) ([07560ed](https://github.com/ydb-platform/ydb-embedded-ui/commit/07560ed8fca8cedbd473f629267522596d1ecb35))
* **package:** export AsideNavigation ([#1262](https://github.com/ydb-platform/ydb-embedded-ui/issues/1262)) ([bb647f5](https://github.com/ydb-platform/ydb-embedded-ui/commit/bb647f5588a7ddcf11b3efa45067b79462159e97))
* paginated table fixes ([#1265](https://github.com/ydb-platform/ydb-embedded-ui/issues/1265)) ([d0ef412](https://github.com/ydb-platform/ydb-embedded-ui/commit/d0ef412197f84542faff019e524855bf81989315))
* use /db_clusters to get base cluster info ([#1251](https://github.com/ydb-platform/ydb-embedded-ui/issues/1251)) ([cd8c7dc](https://github.com/ydb-platform/ydb-embedded-ui/commit/cd8c7dc69364eedfcb490060ca1cdd2d0b4e1e77))
* use groups handler for storage ([#1225](https://github.com/ydb-platform/ydb-embedded-ui/issues/1225)) ([45c0a1e](https://github.com/ydb-platform/ydb-embedded-ui/commit/45c0a1e8dc15eb4a404177a6f02e3e947d9a27e2))


### Bug Fixes

* **autocomplete:** do not lose folder if its name is equals to database ([#1290](https://github.com/ydb-platform/ydb-embedded-ui/issues/1290)) ([f22bdba](https://github.com/ydb-platform/ydb-embedded-ui/commit/f22bdbab6493a07c6150d7a6a52b4064f62f58ac))
* better retry timeout for traces ([#1263](https://github.com/ydb-platform/ydb-embedded-ui/issues/1263)) ([400a526](https://github.com/ydb-platform/ydb-embedded-ui/commit/400a526d9b587563eb977c33e70d0134f41d861d))
* gh-pages commit history ([#1275](https://github.com/ydb-platform/ydb-embedded-ui/issues/1275)) ([f09ccd3](https://github.com/ydb-platform/ydb-embedded-ui/commit/f09ccd3d8fe7fa87b1b162ef386c6dd01f69eb4f))
* **PaginatedStorageNodes:** increase row height ([#1282](https://github.com/ydb-platform/ydb-embedded-ui/issues/1282)) ([08e4308](https://github.com/ydb-platform/ydb-embedded-ui/commit/08e43089fd99ad762d1d6c0300c9c07e5c07dc00))
* unnecessary background refresh on query page ([#1256](https://github.com/ydb-platform/ydb-embedded-ui/issues/1256)) ([04b4313](https://github.com/ydb-platform/ydb-embedded-ui/commit/04b431377e143c002426c39ceca713dbe14cd23e))
* update alter table query template ([#1284](https://github.com/ydb-platform/ydb-embedded-ui/issues/1284)) ([4bf43af](https://github.com/ydb-platform/ydb-embedded-ui/commit/4bf43aff05acd1ab20f670e77b3bdd42c9213d42))

## [6.18.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v6.17.2...v6.18.0) (2024-09-04)


### Features

* **PDiskPage:** add decommission button ([#1168](https://github.com/ydb-platform/ydb-embedded-ui/issues/1168)) ([d3e5f70](https://github.com/ydb-platform/ydb-embedded-ui/commit/d3e5f70a4b71e95f6f42478b979a1157c80e6a36))
* show query trace results ([#1248](https://github.com/ydb-platform/ydb-embedded-ui/issues/1248)) ([2be0f0b](https://github.com/ydb-platform/ydb-embedded-ui/commit/2be0f0b6aa931bf68597370923e7815935db4ba8))
* **Storage:** add advanced disks view ([#1235](https://github.com/ydb-platform/ydb-embedded-ui/issues/1235)) ([3d780aa](https://github.com/ydb-platform/ydb-embedded-ui/commit/3d780aa4c6724f75a811c5d75061c2febd81a4a3))


### Bug Fixes

* **PaginatedTable:** fix table overflow, make column width optional ([#1234](https://github.com/ydb-platform/ydb-embedded-ui/issues/1234)) ([2d06930](https://github.com/ydb-platform/ydb-embedded-ui/commit/2d069305167ffb0b47c2d0e59ed2057dc73e649c))
* **Tablet:** preserve query params when switch tabs ([#1254](https://github.com/ydb-platform/ydb-embedded-ui/issues/1254)) ([c79ccbc](https://github.com/ydb-platform/ydb-embedded-ui/commit/c79ccbc5c9a07b664620c3e3155d24f2754844b5))
* **TopShards:** fix data update on schema object change ([#1245](https://github.com/ydb-platform/ydb-embedded-ui/issues/1245)) ([024fec4](https://github.com/ydb-platform/ydb-embedded-ui/commit/024fec40a4395a31e4780f7ab2206b25d0762e82))

## [6.17.2](https://github.com/ydb-platform/ydb-embedded-ui/compare/v6.17.1...v6.17.2) (2024-09-02)


### Bug Fixes

* add implicit transaction_mode to query settings ([#1240](https://github.com/ydb-platform/ydb-embedded-ui/issues/1240)) ([75a0d89](https://github.com/ydb-platform/ydb-embedded-ui/commit/75a0d892bd30b31da49e1b0e56de87eb13ec00b8))

## [6.17.1](https://github.com/ydb-platform/ydb-embedded-ui/compare/v6.17.0...v6.17.1) (2024-08-30)


### Bug Fixes

* **PaginatedTable:** fix layout ([#1237](https://github.com/ydb-platform/ydb-embedded-ui/issues/1237)) ([fc22275](https://github.com/ydb-platform/ydb-embedded-ui/commit/fc2227553b92ce2b0850a949f1636397b3307784))

## [6.17.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v6.16.0...v6.17.0) (2024-08-29)


### Features

* add setting to add tracing to all requests ([#1218](https://github.com/ydb-platform/ydb-embedded-ui/issues/1218)) ([d7df777](https://github.com/ydb-platform/ydb-embedded-ui/commit/d7df77750d123be881264993c2333c1767a8b66c))
* display all types of keys as plain text ([#1219](https://github.com/ydb-platform/ydb-embedded-ui/issues/1219)) ([13d21cb](https://github.com/ydb-platform/ydb-embedded-ui/commit/13d21cb3bfcca00138e08f83b00cc4068d12dc51))
* **Tablets:** pass database and nodeId params to api handler ([#1199](https://github.com/ydb-platform/ydb-embedded-ui/issues/1199)) ([c049f75](https://github.com/ydb-platform/ydb-embedded-ui/commit/c049f75f0bbd140e40c3caa196a95e0c702ce091))
* **VDiskPage:** use storage table instead of storage info ([#1209](https://github.com/ydb-platform/ydb-embedded-ui/issues/1209)) ([027c668](https://github.com/ydb-platform/ydb-embedded-ui/commit/027c668d917b216646ec15f659707dc146b23d87))


### Bug Fixes

* **AsyncReplicationInfo:** do not show empty fields ([#1189](https://github.com/ydb-platform/ydb-embedded-ui/issues/1189)) ([7b1eff6](https://github.com/ydb-platform/ydb-embedded-ui/commit/7b1eff693c4e4440b96f7a9d2628b916d9d58131))
* incorrect database count with hidden root ([#1206](https://github.com/ydb-platform/ydb-embedded-ui/issues/1206)) ([c0f716f](https://github.com/ydb-platform/ydb-embedded-ui/commit/c0f716f3e30df1ae00aaad231748a8807e395359))
* **Overview:** use nodes?group=Version to get node versions ([#1230](https://github.com/ydb-platform/ydb-embedded-ui/issues/1230)) ([d95ea95](https://github.com/ydb-platform/ydb-embedded-ui/commit/d95ea95c3cd392b9e7052268703b469212c0f508))
* paginated tables not working in safari ([#1231](https://github.com/ydb-platform/ydb-embedded-ui/issues/1231)) ([7162aeb](https://github.com/ydb-platform/ydb-embedded-ui/commit/7162aeb81e1bd453eb8f401a359f922716dd6148))
* remove binary data setting description ([#1180](https://github.com/ydb-platform/ydb-embedded-ui/issues/1180)) ([ec46259](https://github.com/ydb-platform/ydb-embedded-ui/commit/ec46259195e213c0d5c65aca6857ce3b65b32f75))
* set ErrorBoundary max width ([#1221](https://github.com/ydb-platform/ydb-embedded-ui/issues/1221)) ([3191b31](https://github.com/ydb-platform/ydb-embedded-ui/commit/3191b31e0a42970ed4cdd25b21f313381cb57eb5))
* **store:** remove unused params from url mapping ([#1227](https://github.com/ydb-platform/ydb-embedded-ui/issues/1227)) ([be442a0](https://github.com/ydb-platform/ydb-embedded-ui/commit/be442a055a50003ad189a12e372cdf7a2a4d0de8))
* **Versions:** use /nodes instead of /sysinfo to collect versions ([#1220](https://github.com/ydb-platform/ydb-embedded-ui/issues/1220)) ([217d77b](https://github.com/ydb-platform/ydb-embedded-ui/commit/217d77bab69a8e2277de3e4077c3f407b36cd70c))

## [6.16.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v6.15.1...v6.16.0) (2024-08-21)


### Features

* [query settings] display trace level selector ([#1170](https://github.com/ydb-platform/ydb-embedded-ui/issues/1170)) ([f1ab08c](https://github.com/ydb-platform/ydb-embedded-ui/commit/f1ab08c3146135b6fef039ce541311bf2ca73fba))
* **Describe:** add button to copy content to clipboard ([#1192](https://github.com/ydb-platform/ydb-embedded-ui/issues/1192)) ([dc64506](https://github.com/ydb-platform/ydb-embedded-ui/commit/dc64506f3f4fc6a8bf9a1b9a7cad5fc40a12cd38))
* **diagnostics:** render bloom filter better ([#1165](https://github.com/ydb-platform/ydb-embedded-ui/issues/1165)) ([0562b0c](https://github.com/ydb-platform/ydb-embedded-ui/commit/0562b0cf8c2b949ad80a765a2b7bce407b100c4a))
* duration & endtime in queries history ([#1169](https://github.com/ydb-platform/ydb-embedded-ui/issues/1169)) ([f28402c](https://github.com/ydb-platform/ydb-embedded-ui/commit/f28402caed4002f86f265d1b003c2b0b8845eda2))
* enable autorefresh for paginated tables ([#1146](https://github.com/ydb-platform/ydb-embedded-ui/issues/1146)) ([2519c80](https://github.com/ydb-platform/ydb-embedded-ui/commit/2519c80a2807572fe64efbe4360fdcb0f02b533a))
* **Tablet:** redesign tablet page, add data from hive ([#1183](https://github.com/ydb-platform/ydb-embedded-ui/issues/1183)) ([ab3528b](https://github.com/ydb-platform/ydb-embedded-ui/commit/ab3528bd4b1d3a806bd677ac37accb29dc8e5a04))


### Bug Fixes

* default value fixes ([#1166](https://github.com/ydb-platform/ydb-embedded-ui/issues/1166)) ([a210ba1](https://github.com/ydb-platform/ydb-embedded-ui/commit/a210ba17f9f022062e30fc86ca047a245de41313))
* do not reset query results when switch query tabs ([#1198](https://github.com/ydb-platform/ydb-embedded-ui/issues/1198)) ([8be55f1](https://github.com/ydb-platform/ydb-embedded-ui/commit/8be55f1ea710a72b40a30f48a6b20095c89420f0))
* **Nodes:** pass database param to nodes api handler ([#1197](https://github.com/ydb-platform/ydb-embedded-ui/issues/1197)) ([345600f](https://github.com/ydb-platform/ydb-embedded-ui/commit/345600ff227e2f3383dbd9f838ee2221fe31a744))
* preview hides when opening query settings ([#1187](https://github.com/ydb-platform/ydb-embedded-ui/issues/1187)) ([5b80904](https://github.com/ydb-platform/ydb-embedded-ui/commit/5b80904e22ca80383d2e7e0e52d6775da384e2b1))
* use-QueryService-by-default-for-running-queries-1171 ([#1188](https://github.com/ydb-platform/ydb-embedded-ui/issues/1188)) ([62de3e6](https://github.com/ydb-platform/ydb-embedded-ui/commit/62de3e64e1ec139586ce06ae2111137ca2416caf))

## [6.15.1](https://github.com/ydb-platform/ydb-embedded-ui/compare/v6.15.0...v6.15.1) (2024-08-16)


### Bug Fixes

* **api:** do not duplicate request params on retry ([#1105](https://github.com/ydb-platform/ydb-embedded-ui/issues/1105)) ([0580adc](https://github.com/ydb-platform/ydb-embedded-ui/commit/0580adc64a37c6ef6b776a9499a6b10fffa926d9))
* autorefresh control doesnt update info tab in diagnostics ([#1163](https://github.com/ydb-platform/ydb-embedded-ui/issues/1163)) ([ac380ba](https://github.com/ydb-platform/ydb-embedded-ui/commit/ac380ba2394351610d1ca3873825d08b0749f5ab))
* base64 flag both in sendQuery params and body ([#1172](https://github.com/ydb-platform/ydb-embedded-ui/issues/1172)) ([57f393b](https://github.com/ydb-platform/ydb-embedded-ui/commit/57f393b140a9bd4bfad13a48f6184591ba43a997))
* **Preview:** remove wrong and stale styles ([#1162](https://github.com/ydb-platform/ydb-embedded-ui/issues/1162)) ([8134a64](https://github.com/ydb-platform/ydb-embedded-ui/commit/8134a64857bff8d6d76a8a23889dad42ed3c3a66))
* **Tenant:** fix infinite load if describe returns empty data ([#1167](https://github.com/ydb-platform/ydb-embedded-ui/issues/1167)) ([8535629](https://github.com/ydb-platform/ydb-embedded-ui/commit/853562925727e5334750f6e5a2e1e398c2a17623))

## [6.15.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v6.14.1...v6.15.0) (2024-08-14)


### Features

* **Acl:** support InterruptInheritance, fix styles ([#1154](https://github.com/ydb-platform/ydb-embedded-ui/issues/1154)) ([89804af](https://github.com/ydb-platform/ydb-embedded-ui/commit/89804aff8397e2f9ae4fee02396bdaf6acae6de3))
* search in queries history & saved ([#1127](https://github.com/ydb-platform/ydb-embedded-ui/issues/1127)) ([efd5813](https://github.com/ydb-platform/ydb-embedded-ui/commit/efd5813bcea7777eb3f4bde10de3b1f28f724502))
* table preview doesnt update on click ([#1152](https://github.com/ydb-platform/ydb-embedded-ui/issues/1152)) ([512786c](https://github.com/ydb-platform/ydb-embedded-ui/commit/512786ccabcd866a21d69c2e12564c9b198c8132))


### Bug Fixes

* completed query status on page load ([#1132](https://github.com/ydb-platform/ydb-embedded-ui/issues/1132)) ([2fa6922](https://github.com/ydb-platform/ydb-embedded-ui/commit/2fa69229e40103750b7ded1ef828665e7a6aa082))
* **CriticalActionDialog:** increase padding-top ([#1149](https://github.com/ydb-platform/ydb-embedded-ui/issues/1149)) ([aa1f0a2](https://github.com/ydb-platform/ydb-embedded-ui/commit/aa1f0a228a3c95c65e3deada0ea0b11be958022f))
* detect object that looks like axios error ([#1145](https://github.com/ydb-platform/ydb-embedded-ui/issues/1145)) ([085e116](https://github.com/ydb-platform/ydb-embedded-ui/commit/085e116ff62f0ce022c3df047e2273e190fe5999))
* do not show Authentication page before redirect ([#1139](https://github.com/ydb-platform/ydb-embedded-ui/issues/1139)) ([e266c8f](https://github.com/ydb-platform/ydb-embedded-ui/commit/e266c8fc95fef4f9e34d56f82cd57c0b20b89ff6))
* don't show data for un auth user ([#1150](https://github.com/ydb-platform/ydb-embedded-ui/issues/1150)) ([bc17fa4](https://github.com/ydb-platform/ydb-embedded-ui/commit/bc17fa459375611c8a8863a028bc57459410e8df))
* remove tablets from some api handlers ([#1148](https://github.com/ydb-platform/ydb-embedded-ui/issues/1148)) ([b05bff3](https://github.com/ydb-platform/ydb-embedded-ui/commit/b05bff3b20c364b218c3d6f71061570cc6779f39))
* **VDiskInfo:** hide developer ui link for users with viewer rights ([#1137](https://github.com/ydb-platform/ydb-embedded-ui/issues/1137)) ([86104e3](https://github.com/ydb-platform/ydb-embedded-ui/commit/86104e31a6ba335c33a481b322d429205732641b))

## [6.14.1](https://github.com/ydb-platform/ydb-embedded-ui/compare/v6.14.0...v6.14.1) (2024-08-07)


### Bug Fixes

* package build ([#1129](https://github.com/ydb-platform/ydb-embedded-ui/issues/1129)) ([ea8aa29](https://github.com/ydb-platform/ydb-embedded-ui/commit/ea8aa29e8446679bf34f5c5bbc6033cb2482b6b4))

## [6.14.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v6.13.0...v6.14.0) (2024-08-07)


### Features

* copy json query stats ([#1113](https://github.com/ydb-platform/ydb-embedded-ui/issues/1113)) ([e9cfa8a](https://github.com/ydb-platform/ydb-embedded-ui/commit/e9cfa8a7cc8dad7a1dc3fca354419a3af9a852bf))
* display relative schema object path on left overview tab ([#1118](https://github.com/ydb-platform/ydb-embedded-ui/issues/1118)) ([c55164c](https://github.com/ydb-platform/ydb-embedded-ui/commit/c55164c4f6688ee61e520a9eebaa4b2382600a06))
* **e2e:** add warmup for e2e ([#1122](https://github.com/ydb-platform/ydb-embedded-ui/issues/1122)) ([c15dc1a](https://github.com/ydb-platform/ydb-embedded-ui/commit/c15dc1a1a3954cab96abbf76edfb232321485d99))
* remove tablets column from databases table ([#1121](https://github.com/ydb-platform/ydb-embedded-ui/issues/1121)) ([68c3972](https://github.com/ydb-platform/ydb-embedded-ui/commit/68c39726fccb24725bb47aaaad1beadc7d2b26bf))
* stop running query ([#1117](https://github.com/ydb-platform/ydb-embedded-ui/issues/1117)) ([a30258f](https://github.com/ydb-platform/ydb-embedded-ui/commit/a30258f1f362d6a1aa2e098e197fb03d90abee2b))


### Bug Fixes

* **e2e:** use baseurl from config ([#1123](https://github.com/ydb-platform/ydb-embedded-ui/issues/1123)) ([d0efbb1](https://github.com/ydb-platform/ydb-embedded-ui/commit/d0efbb13cbf61accce10e453f408681560d59f3d))
* pass database to topic api handlers ([#1128](https://github.com/ydb-platform/ydb-embedded-ui/issues/1128)) ([b23fc79](https://github.com/ydb-platform/ydb-embedded-ui/commit/b23fc79548b8bdea31d86505bd56ee2baf6d8c1a))
* schema in sendQuery is only query parameter ([#1119](https://github.com/ydb-platform/ydb-embedded-ui/issues/1119)) ([5bb0461](https://github.com/ydb-platform/ydb-embedded-ui/commit/5bb0461b98eb7d5d5deba99199b447abda013fe4))
* **SimplifiedPlan:** should hide dividers if item is collapsed ([#1124](https://github.com/ydb-platform/ydb-embedded-ui/issues/1124)) ([5759e1b](https://github.com/ydb-platform/ydb-embedded-ui/commit/5759e1b5aaa6865eb45f6dda4cbdd0cee91ed533))

## [6.13.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v6.12.0...v6.13.0) (2024-08-02)


### Features

* add flaky tests and bundle size ([#1109](https://github.com/ydb-platform/ydb-embedded-ui/issues/1109)) ([e343a13](https://github.com/ydb-platform/ydb-embedded-ui/commit/e343a13c4cffd7bb6682183208736b3179451056))
* check whoami before other requests ([#1103](https://github.com/ydb-platform/ydb-embedded-ui/issues/1103)) ([70b6dc3](https://github.com/ydb-platform/ydb-embedded-ui/commit/70b6dc3f116aac1a2d86144f5c2a6091c7294190))
* **Diagnostics:** display column's default value in diagnostics schema ([#1102](https://github.com/ydb-platform/ydb-embedded-ui/issues/1102)) ([97013c3](https://github.com/ydb-platform/ydb-embedded-ui/commit/97013c3cc7580feb58e03be4b61cb344319d61a2))
* **ExecuteResult:** show query schema with stats ([#1083](https://github.com/ydb-platform/ydb-embedded-ui/issues/1083)) ([eb6c746](https://github.com/ydb-platform/ydb-embedded-ui/commit/eb6c7462248c5c308bc9c33a183b3a88531495ab))
* **ExecuteResult:** show simplified plan ([#1098](https://github.com/ydb-platform/ydb-embedded-ui/issues/1098)) ([0a18fab](https://github.com/ydb-platform/ydb-embedded-ui/commit/0a18fabdbb9302cdb09b3aac3e14451308f70c38))
* **ExplainResult:** add visualization for simplified query plan ([#1061](https://github.com/ydb-platform/ydb-embedded-ui/issues/1061)) ([73c53dd](https://github.com/ydb-platform/ydb-embedded-ui/commit/73c53dd77a54627df95b2f9148f993b156719fcb))
* move test reports ([#1106](https://github.com/ydb-platform/ydb-embedded-ui/issues/1106)) ([5f8a852](https://github.com/ydb-platform/ydb-embedded-ui/commit/5f8a8522e8cd011e750abf11bb58e966ed37a753))
* option to hide domain ([#1094](https://github.com/ydb-platform/ydb-embedded-ui/issues/1094)) ([8b2fb41](https://github.com/ydb-platform/ydb-embedded-ui/commit/8b2fb412d2744cd470f321d3d944702984cfce60))
* **PDiskPage:** add force restart ([#1073](https://github.com/ydb-platform/ydb-embedded-ui/issues/1073)) ([27f7350](https://github.com/ydb-platform/ydb-embedded-ui/commit/27f7350d60507890ef07d1a640fdea2d795d696f))
* release query settings dialog (and ci test reports) ([#1101](https://github.com/ydb-platform/ydb-embedded-ui/issues/1101)) ([d0ef39c](https://github.com/ydb-platform/ydb-embedded-ui/commit/d0ef39c2456dd2b51897f79efbdddb2fa0e0b8f6))
* settings usage in requests and modification ([#1068](https://github.com/ydb-platform/ydb-embedded-ui/issues/1068)) ([c767175](https://github.com/ydb-platform/ydb-embedded-ui/commit/c767175d483214a4b05c60f26e0be2e2b994eb04))
* show cached data on errors ([#1095](https://github.com/ydb-platform/ydb-embedded-ui/issues/1095)) ([d38f0f0](https://github.com/ydb-platform/ydb-embedded-ui/commit/d38f0f08a677afd8f30c86ced5d032569081eab4))
* use features versions from backend ([#1097](https://github.com/ydb-platform/ydb-embedded-ui/issues/1097)) ([3f041de](https://github.com/ydb-platform/ydb-embedded-ui/commit/3f041de928f48c8f9e31264647ef68066c0f73bd))
* **VDiskPage:** add force evict ([#1093](https://github.com/ydb-platform/ydb-embedded-ui/issues/1093)) ([97dfcfb](https://github.com/ydb-platform/ydb-embedded-ui/commit/97dfcfb318952b33a146d77f2960b7a2d3524752))


### Bug Fixes

* infinite describe requests ([#1086](https://github.com/ydb-platform/ydb-embedded-ui/issues/1086)) ([a8ee1a7](https://github.com/ydb-platform/ydb-embedded-ui/commit/a8ee1a7e62c7bf4b5f5dd828afd39cfcefd84535))
* **Nodes:** better memory bars ([#1091](https://github.com/ydb-platform/ydb-embedded-ui/issues/1091)) ([87f2b92](https://github.com/ydb-platform/ydb-embedded-ui/commit/87f2b92aef40d40ff7af21b19d57b2c2f66528e5))
* **QueryHistory:** query history not saving ([#1111](https://github.com/ydb-platform/ydb-embedded-ui/issues/1111)) ([b5df0d6](https://github.com/ydb-platform/ydb-embedded-ui/commit/b5df0d64c9617d28a912a3e7a8a23e368e3786ba))
* **QuerySettingsForm:** remove excessive updates ([#1099](https://github.com/ydb-platform/ydb-embedded-ui/issues/1099)) ([a41bbff](https://github.com/ydb-platform/ydb-embedded-ui/commit/a41bbffebd0fb6d35ae1b50d5354aa5861800ba3))
* tests not running ([#1108](https://github.com/ydb-platform/ydb-embedded-ui/issues/1108)) ([eb5a627](https://github.com/ydb-platform/ydb-embedded-ui/commit/eb5a627974c6b93c7fa89bc8d3ba7512a262ee4c))

## [6.12.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v6.11.0...v6.12.0) (2024-07-29)


### Features

* add effective ACL ([#1036](https://github.com/ydb-platform/ydb-embedded-ui/issues/1036)) ([bce4e92](https://github.com/ydb-platform/ydb-embedded-ui/commit/bce4e926d6395dcfc53d14e6ea373073ef724145))
* cpu info toggle – display top queries for last hour ([#1049](https://github.com/ydb-platform/ydb-embedded-ui/issues/1049)) ([1fb9078](https://github.com/ydb-platform/ydb-embedded-ui/commit/1fb9078c8ea76c237a5397f74397663eb0b0f7d3))
* **PDiskPage:** add disk space distribution ([#1029](https://github.com/ydb-platform/ydb-embedded-ui/issues/1029)) ([82375c3](https://github.com/ydb-platform/ydb-embedded-ui/commit/82375c39b67a23046e673e4172aa29bbde659ab5))
* **PDiskPage:** add pdisk attributes, display in 2 columns ([#1069](https://github.com/ydb-platform/ydb-embedded-ui/issues/1069)) ([d3b3d9b](https://github.com/ydb-platform/ydb-embedded-ui/commit/d3b3d9bf77dbbe7966bbc072d64cb4c4b5c3d0c4))
* use date pickers from date-components ([#1031](https://github.com/ydb-platform/ydb-embedded-ui/issues/1031)) ([dbb5ba7](https://github.com/ydb-platform/ydb-embedded-ui/commit/dbb5ba797870d738423ebf2f7bfd688d9a9b6d75))
* use monaco-yql-languages for syntax highlight ([#1063](https://github.com/ydb-platform/ydb-embedded-ui/issues/1063)) ([96976aa](https://github.com/ydb-platform/ydb-embedded-ui/commit/96976aaa4f13a3287e3ee8283db77c96703e268e))


### Bug Fixes

* add margin for developer ui button ([#1041](https://github.com/ydb-platform/ydb-embedded-ui/issues/1041)) ([6fa1e0b](https://github.com/ydb-platform/ydb-embedded-ui/commit/6fa1e0bddaa40a972d2160959fba80773d567830))
* do not retry requests by user ([#1059](https://github.com/ydb-platform/ydb-embedded-ui/issues/1059)) ([f4af922](https://github.com/ydb-platform/ydb-embedded-ui/commit/f4af922491faf34e08022dd1e3c9af007a6765d1))
* mark follower is Leader is false ([#1055](https://github.com/ydb-platform/ydb-embedded-ui/issues/1055)) ([c668f52](https://github.com/ydb-platform/ydb-embedded-ui/commit/c668f52171bfc34db4c724341edc9c63a0498aa7))
* **ObjectSummary:** treat EPathTypeSubDomain as EPathTypeExtSubDomain ([#1064](https://github.com/ydb-platform/ydb-embedded-ui/issues/1064)) ([ce0c03e](https://github.com/ydb-platform/ydb-embedded-ui/commit/ce0c03ecfefdc752101b67ae2052e9c1f984ef96))
* pass database param to all handlers inside DB ([#1066](https://github.com/ydb-platform/ydb-embedded-ui/issues/1066)) ([4b34e05](https://github.com/ydb-platform/ydb-embedded-ui/commit/4b34e05ae53ce5c5cab811d37f3a7062ff2f7718))
* **PDiskPage:** move autorefresh to meta level ([#1058](https://github.com/ydb-platform/ydb-embedded-ui/issues/1058)) ([248e57d](https://github.com/ydb-platform/ydb-embedded-ui/commit/248e57de68684e86bb8aa688ac6cceeae45edc7d))
* **PDiskSpaceDistribution:** increase slot height, display 0 id ([#1071](https://github.com/ydb-platform/ydb-embedded-ui/issues/1071)) ([7b9adda](https://github.com/ydb-platform/ydb-embedded-ui/commit/7b9adda65395855625bbcd6a1bd08c0ece81f79b))
* ru.json for datepicker ([#1044](https://github.com/ydb-platform/ydb-embedded-ui/issues/1044)) ([f2dfacb](https://github.com/ydb-platform/ydb-embedded-ui/commit/f2dfacbf5f183dd3ffdb410d9d13acbc59009e74))
* **TableInfo:** update column table info ([#1056](https://github.com/ydb-platform/ydb-embedded-ui/issues/1056)) ([3305cdb](https://github.com/ydb-platform/ydb-embedded-ui/commit/3305cdbcef990ab846a953ae6cf06a88526c0cd3))

## [6.11.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v6.10.3...v6.11.0) (2024-07-19)


### Features

* add data sorting to TopQueries table ([#1028](https://github.com/ydb-platform/ydb-embedded-ui/issues/1028)) ([03608f1](https://github.com/ydb-platform/ydb-embedded-ui/commit/03608f11142a5ef50e140f18bc56cd265553bd4a))
* show developer ui links for hosts and tablets ([#1017](https://github.com/ydb-platform/ydb-embedded-ui/issues/1017)) ([9d198c5](https://github.com/ydb-platform/ydb-embedded-ui/commit/9d198c5477117a8f428acd8a214bf5767166320b))


### Bug Fixes

* **Overview:** use stats from describe for all column tables ([#1026](https://github.com/ydb-platform/ydb-embedded-ui/issues/1026)) ([e1c8f83](https://github.com/ydb-platform/ydb-embedded-ui/commit/e1c8f836be0f577c043121c5c5cb7cbb1f6798e8))
* reduce autocomplete triggers ([#1030](https://github.com/ydb-platform/ydb-embedded-ui/issues/1030)) ([89b4782](https://github.com/ydb-platform/ydb-embedded-ui/commit/89b47823f038a51f63b5c41eae61088e8976df7f))
* **TenantOverview:** add correct tablet storage value ([#1040](https://github.com/ydb-platform/ydb-embedded-ui/issues/1040)) ([c8f3182](https://github.com/ydb-platform/ydb-embedded-ui/commit/c8f3182fc776fac7200d23ce8f34f0b506a95ab3))

## [6.10.3](https://github.com/ydb-platform/ydb-embedded-ui/compare/v6.10.2...v6.10.3) (2024-07-16)


### Bug Fixes

* **Overview:** disable column tables stats request ([#1024](https://github.com/ydb-platform/ydb-embedded-ui/issues/1024)) ([a453d0a](https://github.com/ydb-platform/ydb-embedded-ui/commit/a453d0ab11c055846022c6731480812a0356fd2a))
* **schema:** use one cache entree for tenant path schemas ([#1016](https://github.com/ydb-platform/ydb-embedded-ui/issues/1016)) ([7f283a0](https://github.com/ydb-platform/ydb-embedded-ui/commit/7f283a0f595d587eac3921428d14fe659cf6ee9c))

## [6.10.2](https://github.com/ydb-platform/ydb-embedded-ui/compare/v6.10.1...v6.10.2) (2024-07-10)


### Bug Fixes

* json inspector styles ([#1008](https://github.com/ydb-platform/ydb-embedded-ui/issues/1008)) ([bba245f](https://github.com/ydb-platform/ydb-embedded-ui/commit/bba245fcc4e98252347743fc3807cb15d3aaf836))

## [6.10.1](https://github.com/ydb-platform/ydb-embedded-ui/compare/v6.10.0...v6.10.1) (2024-07-10)


### Bug Fixes

* store types should be stored separately ([#1005](https://github.com/ydb-platform/ydb-embedded-ui/issues/1005)) ([8c2949e](https://github.com/ydb-platform/ydb-embedded-ui/commit/8c2949e81662602d62cf3336546f3093056f46aa))

## [6.10.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v6.9.2...v6.10.0) (2024-07-10)


### Features

* add setting to enable actions with directories from UI ([#997](https://github.com/ydb-platform/ydb-embedded-ui/issues/997)) ([d1fd058](https://github.com/ydb-platform/ydb-embedded-ui/commit/d1fd05855de40fefc1079c1b4bb2d3f7131a89c3))
* **QueryEditor:** save query on Cmd/Ctrl + S ([#944](https://github.com/ydb-platform/ydb-embedded-ui/issues/944)) ([1077e2e](https://github.com/ydb-platform/ydb-embedded-ui/commit/1077e2e2b9e4c83e31633391b0e6f294b80b9f66))
* **UserSettings:** add setting for separate disks pages ([#993](https://github.com/ydb-platform/ydb-embedded-ui/issues/993)) ([18253bd](https://github.com/ydb-platform/ydb-embedded-ui/commit/18253bda3c16b114aa8a7fc5c0575be292d27710))


### Bug Fixes

* **Graph:** fix broken tab for column tables ([#1004](https://github.com/ydb-platform/ydb-embedded-ui/issues/1004)) ([0ac18ea](https://github.com/ydb-platform/ydb-embedded-ui/commit/0ac18ea3915c5d9d36c1fbcbdf59b6f9769d3f51))
* **Query:** fix query layout ([#999](https://github.com/ydb-platform/ydb-embedded-ui/issues/999)) ([2ee3e79](https://github.com/ydb-platform/ydb-embedded-ui/commit/2ee3e79c281bf133ee10dec7f8a463e074098461))
* rename VirtualTable to PaginatedTable, remove old hooks ([#995](https://github.com/ydb-platform/ydb-embedded-ui/issues/995)) ([a9c37d9](https://github.com/ydb-platform/ydb-embedded-ui/commit/a9c37d922b0f2edbb987ff4db4dd6db37631d43b))
* **Settings:** do not display balancer setting in embedded version ([#990](https://github.com/ydb-platform/ydb-embedded-ui/issues/990)) ([9b91080](https://github.com/ydb-platform/ydb-embedded-ui/commit/9b910808e4dbc3a49f3e9a83b179015ba8afb4c4))
* **Storage:** do not request tablets for nodes ([#992](https://github.com/ydb-platform/ydb-embedded-ui/issues/992)) ([b33c1ea](https://github.com/ydb-platform/ydb-embedded-ui/commit/b33c1eae22be46910e5ae08a0e2c16a0d92ea79b))
* **Versions:** return lost copy button ([#1003](https://github.com/ydb-platform/ydb-embedded-ui/issues/1003)) ([a06f1df](https://github.com/ydb-platform/ydb-embedded-ui/commit/a06f1df4416ab71f4fd80797de5734567a3c453b))

## [6.9.2](https://github.com/ydb-platform/ydb-embedded-ui/compare/v6.9.1...v6.9.2) (2024-07-05)


### Bug Fixes

* **Header:** fix state mutation ([#987](https://github.com/ydb-platform/ydb-embedded-ui/issues/987)) ([12ba9c0](https://github.com/ydb-platform/ydb-embedded-ui/commit/12ba9c078ce820267e74f4dc3c290bb989d19e2c))

## [6.9.1](https://github.com/ydb-platform/ydb-embedded-ui/compare/v6.9.0...v6.9.1) (2024-07-05)


### Bug Fixes

* fix package ([#985](https://github.com/ydb-platform/ydb-embedded-ui/issues/985)) ([8eaa897](https://github.com/ydb-platform/ydb-embedded-ui/commit/8eaa897ac9e601d32311490820f3617200c4baf7))

## [6.9.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v6.8.0...v6.9.0) (2024-07-05)


### Features

* add creating directory through context menu in navigation tree ([#958](https://github.com/ydb-platform/ydb-embedded-ui/issues/958)) ([d1902d4](https://github.com/ydb-platform/ydb-embedded-ui/commit/d1902d49f2703ec91b56a6d48d2510533ebbf1a9))
* **AutoRefresh:** use auto refresh control on other pages ([#976](https://github.com/ydb-platform/ydb-embedded-ui/issues/976)) ([6ec3d33](https://github.com/ydb-platform/ydb-embedded-ui/commit/6ec3d333e59a358e6db6045dd6942c91de910f8f))
* prepare for react-router 6, lazy load routes ([#980](https://github.com/ydb-platform/ydb-embedded-ui/issues/980)) ([459a3dd](https://github.com/ydb-platform/ydb-embedded-ui/commit/459a3dde10208a15c7eb6e9fa32f3ed57674aee9))


### Bug Fixes

* **CreateDirectoryDialog:** check path is not empty ([#981](https://github.com/ydb-platform/ydb-embedded-ui/issues/981)) ([5794dcd](https://github.com/ydb-platform/ydb-embedded-ui/commit/5794dcddb8d1908bd3de6c1a67c98011f5568333))
* fix node LoadAverage calculation ([#978](https://github.com/ydb-platform/ydb-embedded-ui/issues/978)) ([191ac71](https://github.com/ydb-platform/ydb-embedded-ui/commit/191ac71f247c9a77765b709365874deefb804af1))
* package update ([#977](https://github.com/ydb-platform/ydb-embedded-ui/issues/977)) ([761b29e](https://github.com/ydb-platform/ydb-embedded-ui/commit/761b29e858be6d60adb438b1e4333be9b0b4ec82))
* **UserSettings:** change default settings values ([#983](https://github.com/ydb-platform/ydb-embedded-ui/issues/983)) ([4dcbb2e](https://github.com/ydb-platform/ydb-embedded-ui/commit/4dcbb2e34316e0714189a085d547b192fb2dc83c))

## [6.8.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v6.7.0...v6.8.0) (2024-07-03)


### Features

* **Acl:** improve view ([#955](https://github.com/ydb-platform/ydb-embedded-ui/issues/955)) ([46aff45](https://github.com/ydb-platform/ydb-embedded-ui/commit/46aff451d7768a44d50d4fe56ff4cedb0be505b0))
* add action "Create async replication" ([#959](https://github.com/ydb-platform/ydb-embedded-ui/issues/959)) ([75b0fa8](https://github.com/ydb-platform/ydb-embedded-ui/commit/75b0fa8532a2a051f3e7042f97d98ca0125a93ae))
* add ReadOnly label to replicated tables ([#970](https://github.com/ydb-platform/ydb-embedded-ui/issues/970)) ([669f7f0](https://github.com/ydb-platform/ydb-embedded-ui/commit/669f7f00e0dc8d0dcbac6f54cf854915a2ca1ac0))


### Bug Fixes

* expand the column 'degraded' width ([#962](https://github.com/ydb-platform/ydb-embedded-ui/issues/962)) ([0d13d53](https://github.com/ydb-platform/ydb-embedded-ui/commit/0d13d53810f4ccbc5aa2545911021126274fbaf8))
* fix replication template ([#973](https://github.com/ydb-platform/ydb-embedded-ui/issues/973)) ([071b0a7](https://github.com/ydb-platform/ydb-embedded-ui/commit/071b0a76b46c860015018c59c961c0dbb7ea3459))
* **TenantNavigation:** fix radio button value ([#969](https://github.com/ydb-platform/ydb-embedded-ui/issues/969)) ([7d85393](https://github.com/ydb-platform/ydb-embedded-ui/commit/7d85393f42e96e567fc670e122d8e94a0f4b6c7b))
* update deps ([#975](https://github.com/ydb-platform/ydb-embedded-ui/issues/975)) ([89703da](https://github.com/ydb-platform/ydb-embedded-ui/commit/89703da7276aaf50b5bd1058ee786210a5867d7d))
* **UserSettings:** fix highlight on search ([#972](https://github.com/ydb-platform/ydb-embedded-ui/issues/972)) ([6fbd52c](https://github.com/ydb-platform/ydb-embedded-ui/commit/6fbd52c2725c1c5313f04b92217356e749ab7fe6))

## [6.7.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v6.6.1...v6.7.0) (2024-06-28)


### Features

* add "Create column table" action ([#957](https://github.com/ydb-platform/ydb-embedded-ui/issues/957)) ([6cb3e2b](https://github.com/ydb-platform/ydb-embedded-ui/commit/6cb3e2b65bd8ed3d9de723fa03ce33848c3f7f05))
* add auto-increment for raw table type ([#929](https://github.com/ydb-platform/ydb-embedded-ui/issues/929)) ([87e22cd](https://github.com/ydb-platform/ydb-embedded-ui/commit/87e22cd2f490f14cdb03a24dd45e6712018ec234))
* **api:** add retries on errors ([#934](https://github.com/ydb-platform/ydb-embedded-ui/issues/934)) ([86faa42](https://github.com/ydb-platform/ydb-embedded-ui/commit/86faa42c49c9c757c5aa75eb65567eae0a34e260))
* **autoRefresh:** move auto refresh to separate reducer ([#943](https://github.com/ydb-platform/ydb-embedded-ui/issues/943)) ([a391e94](https://github.com/ydb-platform/ydb-embedded-ui/commit/a391e9408595d7c6ac3903a13244644475d10ece))
* **autoRefresh:** use user settings to store auto refresh interval ([#956](https://github.com/ydb-platform/ydb-embedded-ui/issues/956)) ([99a24ce](https://github.com/ydb-platform/ydb-embedded-ui/commit/99a24ceadda3623e59743a9c510810994b0c0a42))
* **Header:** use InternalLink for breadcrumb items ([#913](https://github.com/ydb-platform/ydb-embedded-ui/issues/913)) ([cdf36da](https://github.com/ydb-platform/ydb-embedded-ui/commit/cdf36daab24e9a558e5c80f6a8672afd1e84e668))
* **HealthCheck:** show aggregated status in preview ([#839](https://github.com/ydb-platform/ydb-embedded-ui/issues/839)) ([71d316c](https://github.com/ydb-platform/ydb-embedded-ui/commit/71d316c111cdbec2f417fcb54c54144377d326e6))
* **Navigation:** relocate tenant nav to main space ([#936](https://github.com/ydb-platform/ydb-embedded-ui/issues/936)) ([6161d97](https://github.com/ydb-platform/ydb-embedded-ui/commit/6161d97c3c6a7aa0108cb1af8945ada9f039ec00))
* **schema:** use rtk-query ([#948](https://github.com/ydb-platform/ydb-embedded-ui/issues/948)) ([524e815](https://github.com/ydb-platform/ydb-embedded-ui/commit/524e815bc593c60381aa78135bf02b3c2f2b8103))
* **store:** rewrite some reducers to rtk-query ([#942](https://github.com/ydb-platform/ydb-embedded-ui/issues/942)) ([59d41f2](https://github.com/ydb-platform/ydb-embedded-ui/commit/59d41f2868633ae5934d68de44aa538013f9a4ae))
* **user-settings:** sync user settings with LS ([#951](https://github.com/ydb-platform/ydb-embedded-ui/issues/951)) ([9919358](https://github.com/ydb-platform/ydb-embedded-ui/commit/99193580050336b80a6e65544c0d8fcc9294796e))


### Bug Fixes

* handle with undefined value of row and column in issue position ([#932](https://github.com/ydb-platform/ydb-embedded-ui/issues/932)) ([2cb27c5](https://github.com/ydb-platform/ydb-embedded-ui/commit/2cb27c575b9f307e6fe164f7e18a3328b4d5a487))
* rework overview pane in schema browser ([#954](https://github.com/ydb-platform/ydb-embedded-ui/issues/954)) ([ed46a5f](https://github.com/ydb-platform/ydb-embedded-ui/commit/ed46a5f74480813d1eeb3567fae699f4193bd8c7))
* **TableInfo:** scroll ([#946](https://github.com/ydb-platform/ydb-embedded-ui/issues/946)) ([190992e](https://github.com/ydb-platform/ydb-embedded-ui/commit/190992e25fc422e79f8feb68b6a653b24043aa66))
* **Tablets:** enable sort by fqdn ([#947](https://github.com/ydb-platform/ydb-embedded-ui/issues/947)) ([e33dc72](https://github.com/ydb-platform/ydb-embedded-ui/commit/e33dc724045340b2492a6b75325147887582fb7d))
* typo in prettier ([#931](https://github.com/ydb-platform/ydb-embedded-ui/issues/931)) ([e08a36a](https://github.com/ydb-platform/ydb-embedded-ui/commit/e08a36ac67d42691311b9c7ae709f54016ff945d))

## [6.6.1](https://github.com/ydb-platform/ydb-embedded-ui/compare/v6.6.0...v6.6.1) (2024-06-14)


### Bug Fixes

* fix npm package ([#925](https://github.com/ydb-platform/ydb-embedded-ui/issues/925)) ([6a3a594](https://github.com/ydb-platform/ydb-embedded-ui/commit/6a3a5949ccccbe053e063276648ad4322ff9c063))

## [6.6.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v6.5.0...v6.6.0) (2024-06-14)

### Features

* @gravity-ui/websql-autocomplete -&gt; 9.1.0, enable autocomplete by default ([#904](https://github.com/ydb-platform/ydb-embedded-ui/issues/904)) ([e71fad9](https://github.com/ydb-platform/ydb-embedded-ui/commit/e71fad97fe16ec56dd3ee14e2e1f9ac76eb1bed9))
* **Diagnostics:** add tablets tab to all entities with tablets ([#892](https://github.com/ydb-platform/ydb-embedded-ui/issues/892)) ([e94d53a](https://github.com/ydb-platform/ydb-embedded-ui/commit/e94d53a1fb293119dc3383add220da5f0cf601c3))
* **Overview:** add view info ([#912](https://github.com/ydb-platform/ydb-embedded-ui/issues/912)) ([02e0cd6](https://github.com/ydb-platform/ydb-embedded-ui/commit/02e0cd66739b442eed649c9edd257ac1de87dcae))
* **SchemaViewer:** refactor, add view schema ([#910](https://github.com/ydb-platform/ydb-embedded-ui/issues/910)) ([3a10460](https://github.com/ydb-platform/ydb-embedded-ui/commit/3a1046031b0ced98494c821b8dc5bf13b87081c4))
* **UserSettings:** display app version ([#889](https://github.com/ydb-platform/ydb-embedded-ui/issues/889)) ([e52639e](https://github.com/ydb-platform/ydb-embedded-ui/commit/e52639e84dc5e1ca2707bb38ed917cd613a9c395))


### Bug Fixes

* **AsyncReplication:** fix table styles ([#891](https://github.com/ydb-platform/ydb-embedded-ui/issues/891)) ([f879369](https://github.com/ydb-platform/ydb-embedded-ui/commit/f87936951e28e18823c8743b3634f6f7d0d18b55))
* **TenantOverview:** increase section margin ([#916](https://github.com/ydb-platform/ydb-embedded-ui/issues/916)) ([d8f97f0](https://github.com/ydb-platform/ydb-embedded-ui/commit/d8f97f0de7d452a5f741cca023e233968be97bfa))
* revert release, fix package ([#920](https://github.com/ydb-platform/ydb-embedded-ui/issues/920)) ([5d3a7c6](https://github.com/ydb-platform/ydb-embedded-ui/commit/5d3a7c6cb347c6943a84e2938d836fca4da47328))

## [6.5.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v6.4.0...v6.5.0) (2024-06-06)


### Features

* disable controls for users with viewer rights ([#848](https://github.com/ydb-platform/ydb-embedded-ui/issues/848)) ([a15e87c](https://github.com/ydb-platform/ydb-embedded-ui/commit/a15e87c6ceaf5220f23c63dfb6b3b24a02519c96))
* **HotKeys:** add help card ([#861](https://github.com/ydb-platform/ydb-embedded-ui/issues/861)) ([3124bdb](https://github.com/ydb-platform/ydb-embedded-ui/commit/3124bdba5c0186d03f86ceb88d55a2f2c902caec))
* suggest views and table settings ([#864](https://github.com/ydb-platform/ydb-embedded-ui/issues/864)) ([496514b](https://github.com/ydb-platform/ydb-embedded-ui/commit/496514b2e80f2513575a2c5d53bda6b01525f385))
* support async replications ([#856](https://github.com/ydb-platform/ydb-embedded-ui/issues/856)) ([2cecddd](https://github.com/ydb-platform/ydb-embedded-ui/commit/2cecddd837f3864bee300cfd8200291dbd6044bd))
* use existing clusters statuses in filter ([#884](https://github.com/ydb-platform/ydb-embedded-ui/issues/884)) ([bb8cdce](https://github.com/ydb-platform/ydb-embedded-ui/commit/bb8cdcefcaa614ce2b79d324ec08473811ad0417))


### Bug Fixes

* **Query:** parse 200 error response ([#883](https://github.com/ydb-platform/ydb-embedded-ui/issues/883)) ([c4cd541](https://github.com/ydb-platform/ydb-embedded-ui/commit/c4cd541d405da2311ef8292347826bdbeeb808e1))
* **Tenant:** should not duplicate api requests ([#879](https://github.com/ydb-platform/ydb-embedded-ui/issues/879)) ([3bd1e5d](https://github.com/ydb-platform/ydb-embedded-ui/commit/3bd1e5dc26d1535ae7d75eb88548427f6f9aaa45))

## [6.4.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v6.3.0...v6.4.0) (2024-05-31)


### Features

* add Nodes and Storage filters to url ([#849](https://github.com/ydb-platform/ydb-embedded-ui/issues/849)) ([5a789d5](https://github.com/ydb-platform/ydb-embedded-ui/commit/5a789d50d86df2e4fd1918d3bb2e3eb87a1d26dc))
* delete icons sprite, use gravity icons ([#853](https://github.com/ydb-platform/ydb-embedded-ui/issues/853)) ([be1d6c3](https://github.com/ydb-platform/ydb-embedded-ui/commit/be1d6c39ba6818844163aba5267f138150a12a75))
* **Diagnostics:** display tablets as table ([#852](https://github.com/ydb-platform/ydb-embedded-ui/issues/852)) ([7915463](https://github.com/ydb-platform/ydb-embedded-ui/commit/7915463ac0fdeac4266667e5b64f9ed4afa2c174))
* **Node:** add developer ui link for embedded version ([#863](https://github.com/ydb-platform/ydb-embedded-ui/issues/863)) ([163b104](https://github.com/ydb-platform/ydb-embedded-ui/commit/163b10410f54fade05b925cabd932ea597741423))
* **Node:** display tablets as a table ([#855](https://github.com/ydb-platform/ydb-embedded-ui/issues/855)) ([10018ae](https://github.com/ydb-platform/ydb-embedded-ui/commit/10018ae9e2a389f910b4a12f3974af69f05f36d8))


### Bug Fixes

* fix ast always light theme ([#827](https://github.com/ydb-platform/ydb-embedded-ui/issues/827)) ([45084a6](https://github.com/ydb-platform/ydb-embedded-ui/commit/45084a6ea373b8489d1520600303eb1583186fff))
* set 0 for tenant metrics if consumption undefined ([#850](https://github.com/ydb-platform/ydb-embedded-ui/issues/850)) ([313d130](https://github.com/ydb-platform/ydb-embedded-ui/commit/313d1305d1a279f803a77e911ead5006164501db))
* **Tenant:** always set tenantPage in the url ([#859](https://github.com/ydb-platform/ydb-embedded-ui/issues/859)) ([0ee9127](https://github.com/ydb-platform/ydb-embedded-ui/commit/0ee9127596f002e667b5acd1aa7f2ff12149be3d))
* use soft quota for tenant storage ([#837](https://github.com/ydb-platform/ydb-embedded-ui/issues/837)) ([5aa5b68](https://github.com/ydb-platform/ydb-embedded-ui/commit/5aa5b68a619ade6e7b7683d82891d26a36a84ee9))

## [6.3.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v6.2.2...v6.3.0) (2024-05-24)


### Features

* add view to schema ([#834](https://github.com/ydb-platform/ydb-embedded-ui/issues/834)) ([fbeea1a](https://github.com/ydb-platform/ydb-embedded-ui/commit/fbeea1afab22803b6df87fe996b67e3c4e6ba94d))


### Bug Fixes

* @gravity-ui/components -&gt; 3.4.1 ([#845](https://github.com/ydb-platform/ydb-embedded-ui/issues/845)) ([96299d4](https://github.com/ydb-platform/ydb-embedded-ui/commit/96299d4e70281b3535f9d8d8a6ef82c89a257230))

## [6.2.2](https://github.com/ydb-platform/ydb-embedded-ui/compare/v6.2.1...v6.2.2) (2024-05-23)


### Bug Fixes

* fix copy button not hidden after copy ([#838](https://github.com/ydb-platform/ydb-embedded-ui/issues/838)) ([a8d0524](https://github.com/ydb-platform/ydb-embedded-ui/commit/a8d052489d21f5a16ad79d119a7025df3e49c111))

## [6.2.1](https://github.com/ydb-platform/ydb-embedded-ui/compare/v6.2.0...v6.2.1) (2024-05-21)


### Bug Fixes

* fix buttons incorrect align in Nodes and Tenants tables ([#830](https://github.com/ydb-platform/ydb-embedded-ui/issues/830)) ([f97c082](https://github.com/ydb-platform/ydb-embedded-ui/commit/f97c08214c9afce731bc76f6513abbd33101b912))
* **Preview:** send request only for tables ([#832](https://github.com/ydb-platform/ydb-embedded-ui/issues/832)) ([62d8ec9](https://github.com/ydb-platform/ydb-embedded-ui/commit/62d8ec962c281727bd04db5be9b0a12d279f726c))

## [6.2.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v6.1.0...v6.2.0) (2024-05-17)


### Features

* make tables resizeable ([#823](https://github.com/ydb-platform/ydb-embedded-ui/issues/823)) ([ff27390](https://github.com/ydb-platform/ydb-embedded-ui/commit/ff2739033437a069892d4ddf203e98a53ee3a934))
* update deps ([#826](https://github.com/ydb-platform/ydb-embedded-ui/issues/826)) ([e3f08cf](https://github.com/ydb-platform/ydb-embedded-ui/commit/e3f08cf9365587b4fbcd6c151d340b6967622756))

## [6.1.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v6.0.1...v6.0.2) (2024-05-14)


### Features

* improve autocomplete ([#814](https://github.com/ydb-platform/ydb-embedded-ui/issues/814)) ([2615f7e](https://github.com/ydb-platform/ydb-embedded-ui/commit/2615f7e1709ab8280fd1fcd1135f1641d669eaf3))

### Bug Fixes

* **PDiskPage:** explicit error on failed restart ([#821](https://github.com/ydb-platform/ydb-embedded-ui/issues/821)) ([fb81b29](https://github.com/ydb-platform/ydb-embedded-ui/commit/fb81b295073034ac400f08fe7bd26023b1b8a8d8))

## [6.0.1](https://github.com/ydb-platform/ydb-embedded-ui/compare/v6.0.0...v6.0.1) (2024-05-13)


### Bug Fixes

* fix monaco bug in safari ([#812](https://github.com/ydb-platform/ydb-embedded-ui/issues/812)) ([075c5db](https://github.com/ydb-platform/ydb-embedded-ui/commit/075c5db1eea7c73ebea8db3a78ed3253485d564a))

## [6.0.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v5.5.0...v6.0.0) (2024-05-03)


### ⚠ BREAKING CHANGES

* update to uikit 6 ([#789](https://github.com/ydb-platform/ydb-embedded-ui/issues/789))

### Features

* auto refresh with advanced control ([#804](https://github.com/ydb-platform/ydb-embedded-ui/issues/804)) ([bfbd3d0](https://github.com/ydb-platform/ydb-embedded-ui/commit/bfbd3d01083270a362b8db3f858f7b7c2df4fbcd))
* **Cluster:** persist nodes filter in URL ([#797](https://github.com/ydb-platform/ydb-embedded-ui/issues/797)) ([32ef8d6](https://github.com/ydb-platform/ydb-embedded-ui/commit/32ef8d65f53a887a871cff7ef13760bbde6935d6))
* **Diagnostics:** add Schema tab with column family info ([#767](https://github.com/ydb-platform/ydb-embedded-ui/issues/767)) ([504468b](https://github.com/ydb-platform/ydb-embedded-ui/commit/504468bfac2f95ea61d0941058d685ea343f8a9c))
* **healthcheck:** use rtk query ([#800](https://github.com/ydb-platform/ydb-embedded-ui/issues/800)) ([65e25f8](https://github.com/ydb-platform/ydb-embedded-ui/commit/65e25f8c1008cf6e714ada6baa601cd6a876c9ac))
* move some pages to rtk query ([#799](https://github.com/ydb-platform/ydb-embedded-ui/issues/799)) ([876510c](https://github.com/ydb-platform/ydb-embedded-ui/commit/876510ccfd68d99bfae056476a526561101f825a))
* **TenantOverview:** bars instead of circles ([#792](https://github.com/ydb-platform/ydb-embedded-ui/issues/792)) ([05fbeff](https://github.com/ydb-platform/ydb-embedded-ui/commit/05fbeffa9407694bc885b34e36899be32b936382))
* **Tenant:** use rtk query ([#802](https://github.com/ydb-platform/ydb-embedded-ui/issues/802)) ([19487b1](https://github.com/ydb-platform/ydb-embedded-ui/commit/19487b16990dd571c9c21ec9e66609183c7c4de1))
* update to uikit 6 ([#789](https://github.com/ydb-platform/ydb-embedded-ui/issues/789)) ([1a3154d](https://github.com/ydb-platform/ydb-embedded-ui/commit/1a3154de595e6254fe25049373200d40cc41d751))
* use rtk query ([#795](https://github.com/ydb-platform/ydb-embedded-ui/issues/795)) ([9f82408](https://github.com/ydb-platform/ydb-embedded-ui/commit/9f82408e9185a988128ff9f2d6d600cfd6e6325c))


### Bug Fixes

* add cluster dashboard to monitoring link object ([#793](https://github.com/ydb-platform/ydb-embedded-ui/issues/793)) ([b021fe0](https://github.com/ydb-platform/ydb-embedded-ui/commit/b021fe013460328a00f152484947af32ab46e4ee))
* **MetricChart:** spanGaps ([#796](https://github.com/ydb-platform/ydb-embedded-ui/issues/796)) ([33687fb](https://github.com/ydb-platform/ydb-embedded-ui/commit/33687fb5b237347ae1be86192b152b1140dadbb4))
* **QueryEditor:** use `full` stats mode instead of `profile` ([#791](https://github.com/ydb-platform/ydb-embedded-ui/issues/791)) ([6488896](https://github.com/ydb-platform/ydb-embedded-ui/commit/64888960c171934d8ee0a080fb731a7abae79e2c))

## [5.5.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v5.4.0...v5.5.0) (2024-04-02)


### Features

* add databases label ([#774](https://github.com/ydb-platform/ydb-embedded-ui/issues/774)) ([7b8e473](https://github.com/ydb-platform/ydb-embedded-ui/commit/7b8e473845e0769575e3e35a3cd525fdfcdf02b9))
* add VDisk page ([#773](https://github.com/ydb-platform/ydb-embedded-ui/issues/773)) ([ea7ea88](https://github.com/ydb-platform/ydb-embedded-ui/commit/ea7ea884bce3f6660a6c96adced730b6fb6a9e1e))
* **formatNumber:** render numbers with spaces instead of comas ([#778](https://github.com/ydb-platform/ydb-embedded-ui/issues/778)) ([5b42808](https://github.com/ydb-platform/ydb-embedded-ui/commit/5b4280858fb8bc44975bc7a705c15c7152b69c45))


### Bug Fixes

* initial bundle size ([#776](https://github.com/ydb-platform/ydb-embedded-ui/issues/776)) ([b13344d](https://github.com/ydb-platform/ydb-embedded-ui/commit/b13344d3da2a91a6c76733100d34fb3bc3c0f8fa))
* **MetricChart:** zero min value ([#787](https://github.com/ydb-platform/ydb-embedded-ui/issues/787)) ([f767d00](https://github.com/ydb-platform/ydb-embedded-ui/commit/f767d00000666e2a0109a334291406b1bb2a5686))
* **Storage:** use media type for groups ([#788](https://github.com/ydb-platform/ydb-embedded-ui/issues/788)) ([4b46f38](https://github.com/ydb-platform/ydb-embedded-ui/commit/4b46f38bc535971a5e1f9a3ec37f9b1e134084cc))
* **Tablet:** disable restart for stopped tablets ([#780](https://github.com/ydb-platform/ydb-embedded-ui/issues/780)) ([9bcffd6](https://github.com/ydb-platform/ydb-embedded-ui/commit/9bcffd6540e70cdfa93ea22fcd2ca69e97187389))
* **VirtualTable:** make table resizeable by default ([#772](https://github.com/ydb-platform/ydb-embedded-ui/issues/772)) ([fe6ad27](https://github.com/ydb-platform/ydb-embedded-ui/commit/fe6ad27e5b598191d8a3b5c3aeba94ff3377a164))

## [5.4.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v5.3.0...v5.4.0) (2024-03-23)


### Features

* **MetricChart:** display per pool cpu usage ([#769](https://github.com/ydb-platform/ydb-embedded-ui/issues/769)) ([6902afa](https://github.com/ydb-platform/ydb-embedded-ui/commit/6902afab43a635fa70f25aa9149e5c6802ec489b))
* **PDisk:** add restart button ([#766](https://github.com/ydb-platform/ydb-embedded-ui/issues/766)) ([7239727](https://github.com/ydb-platform/ydb-embedded-ui/commit/723972712a3aabfb519ec29a4c191096887d5a1b))


### Bug Fixes

* **MetricChart:** draw area charts ([#764](https://github.com/ydb-platform/ydb-embedded-ui/issues/764)) ([51eb1bf](https://github.com/ydb-platform/ydb-embedded-ui/commit/51eb1bf2a37805012630907774bee8f9b00affff))

## [5.3.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v5.2.1...v5.3.0) (2024-03-15)


### Features

* add PDisk page ([#759](https://github.com/ydb-platform/ydb-embedded-ui/issues/759)) ([c1d3f99](https://github.com/ydb-platform/ydb-embedded-ui/commit/c1d3f996f5b0f1c23cd7ba5e717fad3680ba63af))
* add setting to enable autocomplete, fix constants for completion ([#765](https://github.com/ydb-platform/ydb-embedded-ui/issues/765)) ([88cfc52](https://github.com/ydb-platform/ydb-embedded-ui/commit/88cfc5258eaf64a1b2e5ff6c1403f827e71ed54a))
* add YQL autocomplete ([#755](https://github.com/ydb-platform/ydb-embedded-ui/issues/755)) ([799a05f](https://github.com/ydb-platform/ydb-embedded-ui/commit/799a05fe3301d4be05c33bce8a44eb3018a199c7))
* **MetricChart:** make charts database specific ([#750](https://github.com/ydb-platform/ydb-embedded-ui/issues/750)) ([fa98a22](https://github.com/ydb-platform/ydb-embedded-ui/commit/fa98a2222e5be1d0fe83b90f99d73207eb93fc71))
* use rtk to init store and add typed dispatch ([#749](https://github.com/ydb-platform/ydb-embedded-ui/issues/749)) ([323cb6b](https://github.com/ydb-platform/ydb-embedded-ui/commit/323cb6b5de9204e40af2c2b80ca436edcddb686d))


### Bug Fixes

* add absent deps, update axios ([#756](https://github.com/ydb-platform/ydb-embedded-ui/issues/756)) ([ee723cd](https://github.com/ydb-platform/ydb-embedded-ui/commit/ee723cd8f7122949f388a8380ee05fe96c624ff3))
* add Blue status to EFlag ([#754](https://github.com/ydb-platform/ydb-embedded-ui/issues/754)) ([9a0b867](https://github.com/ydb-platform/ydb-embedded-ui/commit/9a0b867b45262919baaa7032f73cf44d415d3f27))
* fix status colors ([#757](https://github.com/ydb-platform/ydb-embedded-ui/issues/757)) ([9928ee2](https://github.com/ydb-platform/ydb-embedded-ui/commit/9928ee23ee55959eb2162de5c948c321ed849fe7))
* **MetricChart:** explicitly process error with html ([#752](https://github.com/ydb-platform/ydb-embedded-ui/issues/752)) ([6d8a0ba](https://github.com/ydb-platform/ydb-embedded-ui/commit/6d8a0ba454061db4edff948af86731bc6873a356))
* **Tablet:** correctly process error in dialog action ([#758](https://github.com/ydb-platform/ydb-embedded-ui/issues/758)) ([b6bcd68](https://github.com/ydb-platform/ydb-embedded-ui/commit/b6bcd686df4e092019da0d39e9da384930cf4a0a))
* **TEvDescribeSchemeResult:** support null value ([#762](https://github.com/ydb-platform/ydb-embedded-ui/issues/762)) ([72ce541](https://github.com/ydb-platform/ydb-embedded-ui/commit/72ce54159b237f607f971159d0bba4879dfea73c))

## [5.2.1](https://github.com/ydb-platform/ydb-embedded-ui/compare/v5.2.0...v5.2.1) (2024-03-05)


### Bug Fixes

* **Cluster:** make cluster title sticky ([#743](https://github.com/ydb-platform/ydb-embedded-ui/issues/743)) ([823709d](https://github.com/ydb-platform/ydb-embedded-ui/commit/823709d3d12992db86254ccdb4941ead1dc30295))
* display rack for din nodes ([#742](https://github.com/ydb-platform/ydb-embedded-ui/issues/742)) ([54384dd](https://github.com/ydb-platform/ydb-embedded-ui/commit/54384dd2447465109008f4053ba748241a1fa133))
* **QueryEditor:** rename actions ([#741](https://github.com/ydb-platform/ydb-embedded-ui/issues/741)) ([784c5b3](https://github.com/ydb-platform/ydb-embedded-ui/commit/784c5b3ca45aa9be77a25f253b5aae2bb4ecc88b))

## [5.2.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v5.1.0...v5.2.0) (2024-03-04)


### Features

* add binary data in plain text display ([#739](https://github.com/ydb-platform/ydb-embedded-ui/issues/739)) ([dd126b0](https://github.com/ydb-platform/ydb-embedded-ui/commit/dd126b0c03bef61110362596a15b5d069644c232))
* allow replace ErrorBoundary compponent ([#744](https://github.com/ydb-platform/ydb-embedded-ui/issues/744)) ([588c1f1](https://github.com/ydb-platform/ydb-embedded-ui/commit/588c1f165ced6087afc85f535abf3cd08733648d))

## [5.1.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v5.0.0...v5.1.0) (2024-02-29)


### Features

* **Cluster:** move cluster info to overview tab ([#710](https://github.com/ydb-platform/ydb-embedded-ui/issues/710)) ([7abcbc1](https://github.com/ydb-platform/ydb-embedded-ui/commit/7abcbc1148a981bd26240caec0809c45b06c3b98))
* do not use external settings ([#725](https://github.com/ydb-platform/ydb-embedded-ui/issues/725)) ([8af45b0](https://github.com/ydb-platform/ydb-embedded-ui/commit/8af45b073d986b3e6bbd25a0823df7fe78622f09))
* **HotKeys:** revive ([#722](https://github.com/ydb-platform/ydb-embedded-ui/issues/722)) ([8047fe4](https://github.com/ydb-platform/ydb-embedded-ui/commit/8047fe431733f3838021fc0e4074179a8a8a75c4))
* **QueryEditor:** execute query with selected text ([#737](https://github.com/ydb-platform/ydb-embedded-ui/issues/737)) ([122a006](https://github.com/ydb-platform/ydb-embedded-ui/commit/122a0069ba96857cd0377a76cbf744735d287c45))
* show different page titles on different pages ([#733](https://github.com/ydb-platform/ydb-embedded-ui/issues/733)) ([4881f09](https://github.com/ydb-platform/ydb-embedded-ui/commit/4881f090f1c808b3bd0baf2cbf2e71373bb5d641))
* **TopQueries:** add Duration column ([#711](https://github.com/ydb-platform/ydb-embedded-ui/issues/711)) ([3f6a892](https://github.com/ydb-platform/ydb-embedded-ui/commit/3f6a892e42a12a5575253e6e93078195a7212a57))


### Bug Fixes

* configure i18n for libs ([#724](https://github.com/ydb-platform/ydb-embedded-ui/issues/724)) ([c0b7c7d](https://github.com/ydb-platform/ydb-embedded-ui/commit/c0b7c7d21048a35462cf1c4f27a5348e2a9692b1))
* **ExecuteResults:** escape values in table cells when copy as tsv ([#720](https://github.com/ydb-platform/ydb-embedded-ui/issues/720)) ([de40fe6](https://github.com/ydb-platform/ydb-embedded-ui/commit/de40fe611da78fd7f38f429c1f175aed110874b7))
* pass route component props to slot ([#716](https://github.com/ydb-platform/ydb-embedded-ui/issues/716)) ([51e7872](https://github.com/ydb-platform/ydb-embedded-ui/commit/51e7872908f691ef6e84995425b1d169045ebddd))
* **ProgressViewer:** make text more contrast ([#714](https://github.com/ydb-platform/ydb-embedded-ui/issues/714)) ([e09ec79](https://github.com/ydb-platform/ydb-embedded-ui/commit/e09ec792a99df38e7f742defe03bcc8850967abe))
* remove console log on monitoring data parsing error ([#723](https://github.com/ydb-platform/ydb-embedded-ui/issues/723)) ([f73dbc8](https://github.com/ydb-platform/ydb-embedded-ui/commit/f73dbc8a735179e8466d6fbd08325cdf76790e1d))
* **TopQueries:** use EndTime ([#727](https://github.com/ydb-platform/ydb-embedded-ui/issues/727)) ([5e708ca](https://github.com/ydb-platform/ydb-embedded-ui/commit/5e708cad9c98530e64dbebd195edb43fea360830))
* update tenant cpu ([#736](https://github.com/ydb-platform/ydb-embedded-ui/issues/736)) ([775bacf](https://github.com/ydb-platform/ydb-embedded-ui/commit/775bacfda549e5cea09ea03069908a59c12c0c3e))
* update tenant storage ([#726](https://github.com/ydb-platform/ydb-embedded-ui/issues/726)) ([dc0df8a](https://github.com/ydb-platform/ydb-embedded-ui/commit/dc0df8ab09d6a3953279d6be52d245a64c78680c))
* use DC and Rack from Location for nodes ([#713](https://github.com/ydb-platform/ydb-embedded-ui/issues/713)) ([e64c3b0](https://github.com/ydb-platform/ydb-embedded-ui/commit/e64c3b065dd07f8c2b1b7f131e52ed01e2f579fb))

## [5.0.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v4.33.0...v5.0.0) (2024-02-13)


### Features

* add multi clusters support ([#701](https://github.com/ydb-platform/ydb-embedded-ui/issues/701)) ([429aa0e](https://github.com/ydb-platform/ydb-embedded-ui/commit/429aa0e2138c4635f5c0ab26ba07901ec0d0162d))

## [4.33.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v4.32.0...v4.33.0) (2024-02-07)


### Features

* **VirtualTable:** enable columns resize ([#697](https://github.com/ydb-platform/ydb-embedded-ui/issues/697)) ([907b275](https://github.com/ydb-platform/ydb-embedded-ui/commit/907b2751541575cde3effcf5359a19cd9b6adffa))


### Bug Fixes

* **TableInfo:** format partitions count ([#704](https://github.com/ydb-platform/ydb-embedded-ui/issues/704)) ([2271535](https://github.com/ydb-platform/ydb-embedded-ui/commit/2271535964e4dcf72a3205f61769abfbb22543d4))
* **TenantDashboard:** hide if charts not enabled ([#675](https://github.com/ydb-platform/ydb-embedded-ui/issues/675)) ([fe0cad4](https://github.com/ydb-platform/ydb-embedded-ui/commit/fe0cad4e37e6b9dc8b8e9b5aeca882e798ec0cce))

## [4.32.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v4.31.2...v4.32.0) (2024-02-07)


### Features

* use VirtualTable in Nodes and Diagnostics ([#678](https://github.com/ydb-platform/ydb-embedded-ui/issues/678)) ([9158050](https://github.com/ydb-platform/ydb-embedded-ui/commit/91580507abf8dd4ac7d2ce070f83e9838ddd4bda))

## [4.31.2](https://github.com/ydb-platform/ydb-embedded-ui/compare/v4.31.1...v4.31.2) (2024-02-01)


### Bug Fixes

* **VirtualTable:** optimise requests ([#676](https://github.com/ydb-platform/ydb-embedded-ui/issues/676)) ([9a50a34](https://github.com/ydb-platform/ydb-embedded-ui/commit/9a50a34110eeeeddc0bf83cc5626bca30804ac1a))

## [4.31.1](https://github.com/ydb-platform/ydb-embedded-ui/compare/v4.31.0...v4.31.1) (2024-02-01)


### Bug Fixes

* **MetricChart:** do not convert nulls ([#677](https://github.com/ydb-platform/ydb-embedded-ui/issues/677)) ([c51c7aa](https://github.com/ydb-platform/ydb-embedded-ui/commit/c51c7aa1b6fb84342ab38466726175077cd7ef2e))

## [4.31.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v4.31.0...v4.31.0) (2024-01-31)


### Features

* **TenantOverview:** add charts ([#657](https://github.com/ydb-platform/ydb-embedded-ui/issues/657)) ([78daa0b](https://github.com/ydb-platform/ydb-embedded-ui/commit/78daa0bc5a1eb66d0bb0b88ccb559335294e44a7))

## [4.30.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v4.29.0...v4.30.0) (2024-01-16)


### Features

* add clipboard button to nodes tree titles ([#648](https://github.com/ydb-platform/ydb-embedded-ui/issues/648)) ([1411651](https://github.com/ydb-platform/ydb-embedded-ui/commit/141165173189be064e9e9314b42aa3eb7fce9c69))

## [4.29.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v4.28.0...v4.29.0) (2024-01-12)


### Features

* add ErrorBoundary ([#549](https://github.com/ydb-platform/ydb-embedded-ui/issues/549)) ([f5ad224](https://github.com/ydb-platform/ydb-embedded-ui/commit/f5ad224b342e0fa25b1bafa3f5e2202ce165ef80))

## [4.28.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v4.27.1...v4.28.0) (2024-01-10)


### Features

* **Storage:** use VirtualTable ([#628](https://github.com/ydb-platform/ydb-embedded-ui/issues/628)) ([67fd9b0](https://github.com/ydb-platform/ydb-embedded-ui/commit/67fd9b03653dd28be650094c987451b09fcce858))

## [4.27.1](https://github.com/ydb-platform/ydb-embedded-ui/compare/v4.27.0...v4.27.1) (2024-01-10)


### Bug Fixes

* enable extract and set user settings manually ([#629](https://github.com/ydb-platform/ydb-embedded-ui/issues/629)) ([5eecd58](https://github.com/ydb-platform/ydb-embedded-ui/commit/5eecd58249688b4a8b3ecad766564f7b404e839c))

## [4.27.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v4.26.0...v4.27.0) (2023-12-28)


### Features

* migrate from external settings api ([#621](https://github.com/ydb-platform/ydb-embedded-ui/issues/621)) ([ae2fbbe](https://github.com/ydb-platform/ydb-embedded-ui/commit/ae2fbbef66d9aba150012027baf8b89bf79cd741))

## [4.26.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v4.25.0...v4.26.0) (2023-12-14)


### Features

* update to uikit5 ([#607](https://github.com/ydb-platform/ydb-embedded-ui/issues/607)) ([ddd263b](https://github.com/ydb-platform/ydb-embedded-ui/commit/ddd263bd39d4de262e6c891dce6c6ff6ba2a3379))

## [4.25.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v4.24.0...v4.25.0) (2023-12-07)


### Features

* **Diagnostics:** remove tenant diagnostics cards setting ([#602](https://github.com/ydb-platform/ydb-embedded-ui/issues/602)) ([fe61df8](https://github.com/ydb-platform/ydb-embedded-ui/commit/fe61df86048013432c4e4788d1e621298ecb1fb2))
* **Query:** remove query modes setting ([#600](https://github.com/ydb-platform/ydb-embedded-ui/issues/600)) ([78c63e4](https://github.com/ydb-platform/ydb-embedded-ui/commit/78c63e4a2e60950970914eaba49304b68aad0f80))

## [4.24.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v4.23.0...v4.24.0) (2023-12-07)


### Features

* always use localStorage if no settingsApi ([#603](https://github.com/ydb-platform/ydb-embedded-ui/issues/603)) ([ff692df](https://github.com/ydb-platform/ydb-embedded-ui/commit/ff692dffa99d278f6b261bbf1aac0ee24c661a6d))

## [4.23.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v4.22.0...v4.23.0) (2023-12-06)


### Features

* **ClusterInfo:** display groups stats ([#598](https://github.com/ydb-platform/ydb-embedded-ui/issues/598)) ([c31d048](https://github.com/ydb-platform/ydb-embedded-ui/commit/c31d0480a1b91cf01a660fd1d9726c6708f7c252))
* **TenantOverview:** add links to sections titles ([#599](https://github.com/ydb-platform/ydb-embedded-ui/issues/599)) ([30401fa](https://github.com/ydb-platform/ydb-embedded-ui/commit/30401fa354d90943bc4af4ddbf65466ce10381f9))


### Bug Fixes

* **Schema:** display keys in right order ([#596](https://github.com/ydb-platform/ydb-embedded-ui/issues/596)) ([c99b7e2](https://github.com/ydb-platform/ydb-embedded-ui/commit/c99b7e2e97acffc1cab450dfbf758c38b8b6e4d5))

## [4.22.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v4.21.1...v4.22.0) (2023-11-27)


### Features

* **Query:** enable queries with multiple resultsets ([#595](https://github.com/ydb-platform/ydb-embedded-ui/issues/595)) ([2eedfb6](https://github.com/ydb-platform/ydb-embedded-ui/commit/2eedfb6ec3be932c7399bb67de901798c0b31b50))
* **TenantOverview:** add columns to memory table ([#593](https://github.com/ydb-platform/ydb-embedded-ui/issues/593)) ([6379577](https://github.com/ydb-platform/ydb-embedded-ui/commit/6379577782cfa69de9fb39640d2a143f1670be39))


### Bug Fixes

* fix disks developer UI links for paths with nodeId ([#594](https://github.com/ydb-platform/ydb-embedded-ui/issues/594)) ([7f5a783](https://github.com/ydb-platform/ydb-embedded-ui/commit/7f5a78393d0c23e584ad73040fd0e73d404e5d01))
* **TopShards:** sort by InFlightTxCount ([#591](https://github.com/ydb-platform/ydb-embedded-ui/issues/591)) ([eb3592d](https://github.com/ydb-platform/ydb-embedded-ui/commit/eb3592d69a465814de27e8b1e368b34cc60fed2f))

## [4.21.1](https://github.com/ydb-platform/ydb-embedded-ui/compare/v4.21.0...v4.21.1) (2023-11-17)


### Bug Fixes

* move inverted disk space setting to general page ([#589](https://github.com/ydb-platform/ydb-embedded-ui/issues/589)) ([b09345e](https://github.com/ydb-platform/ydb-embedded-ui/commit/b09345e1ebe9e7a47beea5ab2dac4257790232cc))
* **Nodes:** always use nodes when flag is enabled ([#584](https://github.com/ydb-platform/ydb-embedded-ui/issues/584)) ([6ac6ee2](https://github.com/ydb-platform/ydb-embedded-ui/commit/6ac6ee2516bb2612cc7832e67ffa0bf92615a36c))
* **QueryResultTable:** fix table error on null cell sort ([#590](https://github.com/ydb-platform/ydb-embedded-ui/issues/590)) ([805a339](https://github.com/ydb-platform/ydb-embedded-ui/commit/805a339b0bba34412bf8e854cf6d24ae7c080539))
* **Tablets:** reduce rerenders ([#585](https://github.com/ydb-platform/ydb-embedded-ui/issues/585)) ([f1767a1](https://github.com/ydb-platform/ydb-embedded-ui/commit/f1767a143b139de4cd7c0df5c8c243c0224ebd3c))
* turn on query modes and metrics cards by default ([#588](https://github.com/ydb-platform/ydb-embedded-ui/issues/588)) ([c2f0d74](https://github.com/ydb-platform/ydb-embedded-ui/commit/c2f0d7424cb3182926f125a1e8c16cd4a2d422b9))
* update links to VDisk and PDisk Developer UI ([#582](https://github.com/ydb-platform/ydb-embedded-ui/issues/582)) ([97dda88](https://github.com/ydb-platform/ydb-embedded-ui/commit/97dda88bd595295eefaed4c0cbcd333e84b047f0))
* **VirtualNodes:** display developerUI link on hover ([#587](https://github.com/ydb-platform/ydb-embedded-ui/issues/587)) ([ba6c249](https://github.com/ydb-platform/ydb-embedded-ui/commit/ba6c249a9793b0bac45607b0b36f284dea4e0a7a))

## [4.21.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v4.20.4...v4.21.0) (2023-10-27)


### Features

* add VirtualTable, use for Nodes ([#578](https://github.com/ydb-platform/ydb-embedded-ui/issues/578)) ([d6197d4](https://github.com/ydb-platform/ydb-embedded-ui/commit/d6197d4bebf509596dfff4e1b4a7fe51a847424e))

## [4.20.4](https://github.com/ydb-platform/ydb-embedded-ui/compare/v4.20.3...v4.20.4) (2023-10-27)


### Bug Fixes

* **Storage:** set storage true for nodes ([#579](https://github.com/ydb-platform/ydb-embedded-ui/issues/579)) ([146d235](https://github.com/ydb-platform/ydb-embedded-ui/commit/146d23563ef50461260f13eedf66ad7da9f76c8a))

## [4.20.3](https://github.com/ydb-platform/ydb-embedded-ui/compare/v4.20.2...v4.20.3) (2023-10-25)


### Bug Fixes

* fix port doesnt match ([#576](https://github.com/ydb-platform/ydb-embedded-ui/issues/576)) ([147a2a9](https://github.com/ydb-platform/ydb-embedded-ui/commit/147a2a919c5fe8ec99f19620da70387ab6c0e519))

## [4.20.2](https://github.com/ydb-platform/ydb-embedded-ui/compare/v4.20.1...v4.20.2) (2023-10-25)


### Bug Fixes

* fix diagnostics top queries width ([#574](https://github.com/ydb-platform/ydb-embedded-ui/issues/574)) ([afa17f2](https://github.com/ydb-platform/ydb-embedded-ui/commit/afa17f236331692167a0a37936b090a8baa772df))
* fix sticky storage info ([#573](https://github.com/ydb-platform/ydb-embedded-ui/issues/573)) ([4b923d1](https://github.com/ydb-platform/ydb-embedded-ui/commit/4b923d1db73c53c63e95f43487127b4c2c1e4cac))
* use UsageLabel in top groups by usage table ([#572](https://github.com/ydb-platform/ydb-embedded-ui/issues/572)) ([752888d](https://github.com/ydb-platform/ydb-embedded-ui/commit/752888d26ac2cab75307011fb1354830b1cb6db6))

## [4.20.1](https://github.com/ydb-platform/ydb-embedded-ui/compare/v4.20.0...v4.20.1) (2023-10-24)


### Bug Fixes

* fix createExternalUILink ([#571](https://github.com/ydb-platform/ydb-embedded-ui/issues/571)) ([52546f1](https://github.com/ydb-platform/ydb-embedded-ui/commit/52546f17dbfdb255b2429836e880d6812b19d66a))
* fix incorrect truncate strings with popover ([#567](https://github.com/ydb-platform/ydb-embedded-ui/issues/567)) ([d82e65b](https://github.com/ydb-platform/ydb-embedded-ui/commit/d82e65b925b76dc539a76520eccf601951654e94))
* fix top queries table row height ([#565](https://github.com/ydb-platform/ydb-embedded-ui/issues/565)) ([b12dceb](https://github.com/ydb-platform/ydb-embedded-ui/commit/b12dcebdb0167fd5852c247bca48844ef6ab35af))
* refactor metrics storage section ([#568](https://github.com/ydb-platform/ydb-embedded-ui/issues/568)) ([db5d922](https://github.com/ydb-platform/ydb-embedded-ui/commit/db5d922d06b88c9d8a792220d2a178c81806c09e))
* update @types/react ([#570](https://github.com/ydb-platform/ydb-embedded-ui/issues/570)) ([1e38c5b](https://github.com/ydb-platform/ydb-embedded-ui/commit/1e38c5bb3b4b2139b2141636d6434c2a2ec65772))

## [4.20.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v4.19.3...v4.20.0) (2023-10-19)


### Features

* add top nodes by memory table ([#562](https://github.com/ydb-platform/ydb-embedded-ui/issues/562)) ([0d4ccf2](https://github.com/ydb-platform/ydb-embedded-ui/commit/0d4ccf2a85251fadad66182ab7d6ccd54a58e6cf))
* add top tables links ([#564](https://github.com/ydb-platform/ydb-embedded-ui/issues/564)) ([e9b918f](https://github.com/ydb-platform/ydb-embedded-ui/commit/e9b918f0abace890cfafd9d7b219be5b69cac820))
* **Storage:** hide nodes table for node page ([#557](https://github.com/ydb-platform/ydb-embedded-ui/issues/557)) ([9a25a00](https://github.com/ydb-platform/ydb-embedded-ui/commit/9a25a002b28824f7e616ac8143dbde12de0b0fb7))
* **TenantOverview:** add cpu tab to tenant diagnostics ([#550](https://github.com/ydb-platform/ydb-embedded-ui/issues/550)) ([3048f84](https://github.com/ydb-platform/ydb-embedded-ui/commit/3048f8478d97249da4f7b66c26ed55f6f21e0f81))


### Bug Fixes

* add loader for healthcheck ([#563](https://github.com/ydb-platform/ydb-embedded-ui/issues/563)) ([6caea3d](https://github.com/ydb-platform/ydb-embedded-ui/commit/6caea3dec8f901090b0f8f7c1796880d7dc90a99))
* **LinkToSchemaObject:** fix schema link ([#566](https://github.com/ydb-platform/ydb-embedded-ui/issues/566)) ([6ca8a70](https://github.com/ydb-platform/ydb-embedded-ui/commit/6ca8a705b6ddacb1f845aabb7761fd22c0c3b4e0))

## [4.19.3](https://github.com/ydb-platform/ydb-embedded-ui/compare/v4.19.2...v4.19.3) (2023-10-13)


### Bug Fixes

* fix ProgressViewer background ([#556](https://github.com/ydb-platform/ydb-embedded-ui/issues/556)) ([6234462](https://github.com/ydb-platform/ydb-embedded-ui/commit/62344629713059fdfb191d3b8a57742f864dad66))
* **Storage:** display all groups by default ([#554](https://github.com/ydb-platform/ydb-embedded-ui/issues/554)) ([1da83f1](https://github.com/ydb-platform/ydb-embedded-ui/commit/1da83f19661ed8e49dd7c8a0930ce89a7c8c0185))

## [4.19.2](https://github.com/ydb-platform/ydb-embedded-ui/compare/v4.19.1...v4.19.2) (2023-10-12)


### Bug Fixes

* add default data formatter to ProgressViewer ([#552](https://github.com/ydb-platform/ydb-embedded-ui/issues/552)) ([ac372a4](https://github.com/ydb-platform/ydb-embedded-ui/commit/ac372a415e67e7126518d9c5a8d04594b82cf485))
* **Tenant:** fix tree not fully collapsed bug ([#551](https://github.com/ydb-platform/ydb-embedded-ui/issues/551)) ([8469307](https://github.com/ydb-platform/ydb-embedded-ui/commit/8469307b67d463ed2aafd17b2c0319ea40c1f8d5))

## [4.19.1](https://github.com/ydb-platform/ydb-embedded-ui/compare/v4.19.0...v4.19.1) (2023-10-11)


### Bug Fixes

* add storage value to tb formatter ([#547](https://github.com/ydb-platform/ydb-embedded-ui/issues/547)) ([f1e4377](https://github.com/ydb-platform/ydb-embedded-ui/commit/f1e4377443be493a7072aca33a62b51e381f6841))

## [4.19.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v4.18.0...v4.19.0) (2023-10-11)


### Features

* **TenantOverview:** add storage tab to tenant diagnostics ([#541](https://github.com/ydb-platform/ydb-embedded-ui/issues/541)) ([c4cdd35](https://github.com/ydb-platform/ydb-embedded-ui/commit/c4cdd354cd9780dfd7dfee80ec225f59d4230625))


### Bug Fixes

* add NodeId to NodeAddress type ([#545](https://github.com/ydb-platform/ydb-embedded-ui/issues/545)) ([3df82d3](https://github.com/ydb-platform/ydb-embedded-ui/commit/3df82d39466696ec61e34b915b355dacd0482ebc))
* display database name in node info ([#543](https://github.com/ydb-platform/ydb-embedded-ui/issues/543)) ([788ad9a](https://github.com/ydb-platform/ydb-embedded-ui/commit/788ad9a7a1a56ffe93ec7e4861ded6cceef72d9c))
* fix cpu usage calculation ([#542](https://github.com/ydb-platform/ydb-embedded-ui/issues/542)) ([f46b03d](https://github.com/ydb-platform/ydb-embedded-ui/commit/f46b03d6157f19017560d71a9ab6591f045bad96))
* fix incorrect data display in ProgressViewer ([#546](https://github.com/ydb-platform/ydb-embedded-ui/issues/546)) ([be077b8](https://github.com/ydb-platform/ydb-embedded-ui/commit/be077b83a4b4cf083d506e77abf0f2b6570c87d3))

## [4.18.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v4.17.0...v4.18.0) (2023-09-25)


### Features

* **ProgressViewer:** add custom threasholds to ProgressViewer ([#540](https://github.com/ydb-platform/ydb-embedded-ui/issues/540)) ([3553065](https://github.com/ydb-platform/ydb-embedded-ui/commit/35530655581357f4a79c277a5bf9846b3befb784))


### Bug Fixes

* **Authentication:** enable page redirect ([#539](https://github.com/ydb-platform/ydb-embedded-ui/issues/539)) ([721883c](https://github.com/ydb-platform/ydb-embedded-ui/commit/721883cc7f4ca60e64d4a5f77b939dbb8e960855))
* **Healthcheck:** add merge_records request param ([#538](https://github.com/ydb-platform/ydb-embedded-ui/issues/538)) ([6a47481](https://github.com/ydb-platform/ydb-embedded-ui/commit/6a474814f71c3318715a8ce638fd522a770d8038))
* **Nodes:** use nodes endpoint by default ([#535](https://github.com/ydb-platform/ydb-embedded-ui/issues/535)) ([12d4fef](https://github.com/ydb-platform/ydb-embedded-ui/commit/12d4fefde7a6663bb1a11f46b4e94fb24b23e966))
* rename flag for display metrics cards for database diagnostics ([#536](https://github.com/ydb-platform/ydb-embedded-ui/issues/536)) ([957e1fa](https://github.com/ydb-platform/ydb-embedded-ui/commit/957e1fafbbc43928498ae9e8d0bc119bcda5288d))

## [4.17.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v4.16.2...v4.17.0) (2023-09-18)


### Features

* add sorting of issues in issues tree ([#532](https://github.com/ydb-platform/ydb-embedded-ui/issues/532)) ([9f7837c](https://github.com/ydb-platform/ydb-embedded-ui/commit/9f7837c95bd1132dd287011e1aadc96c0819b40d))
* move healthcheck to tabs ([#531](https://github.com/ydb-platform/ydb-embedded-ui/issues/531)) ([1879d3d](https://github.com/ydb-platform/ydb-embedded-ui/commit/1879d3d8f717a0baaec0d506ad354d81a226fa62))
* update TenantOverview design ([#527](https://github.com/ydb-platform/ydb-embedded-ui/issues/527)) ([8a752e0](https://github.com/ydb-platform/ydb-embedded-ui/commit/8a752e0def3dc4317fd18519aed210bdc23fefa2))


### Bug Fixes

* fix Healthcheck blinking ([#528](https://github.com/ydb-platform/ydb-embedded-ui/issues/528)) ([0fc6c46](https://github.com/ydb-platform/ydb-embedded-ui/commit/0fc6c46eb15aeb73a984ba2c2cbe18ef7116382e))
* **Tenants:** use blob storage ([#530](https://github.com/ydb-platform/ydb-embedded-ui/issues/530)) ([8a546a1](https://github.com/ydb-platform/ydb-embedded-ui/commit/8a546a1ab2f812acc1523c1c35738f4c605c32a5))

## [4.16.2](https://github.com/ydb-platform/ydb-embedded-ui/compare/v4.16.1...v4.16.2) (2023-08-28)


### Bug Fixes

* fix topic templates ([#524](https://github.com/ydb-platform/ydb-embedded-ui/issues/524)) ([f593b57](https://github.com/ydb-platform/ydb-embedded-ui/commit/f593b575fb64d0c69b56e743fd4cd6faba1e9d0e))
* rename additionalInfo params to additionalProps ([#525](https://github.com/ydb-platform/ydb-embedded-ui/issues/525)) ([dd2b040](https://github.com/ydb-platform/ydb-embedded-ui/commit/dd2b04039cd80072fe11744f3490c176fe21b16b))

## [4.16.1](https://github.com/ydb-platform/ydb-embedded-ui/compare/v4.16.0...v4.16.1) (2023-08-25)


### Bug Fixes

* fix types for external props ([#522](https://github.com/ydb-platform/ydb-embedded-ui/issues/522)) ([173081f](https://github.com/ydb-platform/ydb-embedded-ui/commit/173081f2f0d2814b2311757988d91fbffc2a509f))

## [4.16.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v4.15.1...v4.16.0) (2023-08-25)


### Features

* add language setting ([#520](https://github.com/ydb-platform/ydb-embedded-ui/issues/520)) ([425c9ae](https://github.com/ydb-platform/ydb-embedded-ui/commit/425c9ae1fed83d7695d2a9288c2ef24c2807d8da))
* **Diagnostics:** update Healthcheck design ([#509](https://github.com/ydb-platform/ydb-embedded-ui/issues/509)) ([e315ca4](https://github.com/ydb-platform/ydb-embedded-ui/commit/e315ca42ac6c9d1736aaa25e2dd90afc2bcb9a8e))
* **Query:** support PostgreSQL syntax ([#515](https://github.com/ydb-platform/ydb-embedded-ui/issues/515)) ([0c8346e](https://github.com/ydb-platform/ydb-embedded-ui/commit/0c8346efc3643a8d201137901880f985dc100458))


### Bug Fixes

* **UserSettings:** update query mode setting description ([#521](https://github.com/ydb-platform/ydb-embedded-ui/issues/521)) ([c526471](https://github.com/ydb-platform/ydb-embedded-ui/commit/c52647192ff95d8fb9961479a85cc4d5a639d4e6))

## [4.15.1](https://github.com/ydb-platform/ydb-embedded-ui/compare/v4.15.0...v4.15.1) (2023-08-21)


### Bug Fixes

* **SchemaTree:** update create table template ([#512](https://github.com/ydb-platform/ydb-embedded-ui/issues/512)) ([712b3f3](https://github.com/ydb-platform/ydb-embedded-ui/commit/712b3f3612b09fdc5c850ffc3a984cd86827e5b9))

## [4.15.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v4.14.0...v4.15.0) (2023-08-17)


### Features

* **SchemaTree:** add actions for topic ([#507](https://github.com/ydb-platform/ydb-embedded-ui/issues/507)) ([6700136](https://github.com/ydb-platform/ydb-embedded-ui/commit/670013629cb68425e670969323a2ef466ef7c018))
* **Storage:** sort on backend ([#510](https://github.com/ydb-platform/ydb-embedded-ui/issues/510)) ([034a89a](https://github.com/ydb-platform/ydb-embedded-ui/commit/034a89a9844021c5ea3a73c8f6456e35128078c0))
* **Storage:** v2 api and backend filters ([#506](https://github.com/ydb-platform/ydb-embedded-ui/issues/506)) ([ce4bf6d](https://github.com/ydb-platform/ydb-embedded-ui/commit/ce4bf6d0ef154b87a7b3a44d56281230b2b5b554))

## [4.14.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v4.13.0...v4.14.0) (2023-08-11)


### Features

* **Nodes:** filter and sort on backend ([#503](https://github.com/ydb-platform/ydb-embedded-ui/issues/503)) ([2e8ab8e](https://github.com/ydb-platform/ydb-embedded-ui/commit/2e8ab8e9965db61ec281f7340b89dd3967b639df))
* **Query:** add explanation to query duration ([#501](https://github.com/ydb-platform/ydb-embedded-ui/issues/501)) ([a5f5140](https://github.com/ydb-platform/ydb-embedded-ui/commit/a5f5140a23864147d8495e3c6b94709e5e710a9b))


### Bug Fixes

* **Header:** add icons for nodes and tablets ([#500](https://github.com/ydb-platform/ydb-embedded-ui/issues/500)) ([862660c](https://github.com/ydb-platform/ydb-embedded-ui/commit/862660c1928c2c2b626e4417cd043f0bd5a65df9))
* **Query:** fix query method selector help text ([#504](https://github.com/ydb-platform/ydb-embedded-ui/issues/504)) ([65cdf9e](https://github.com/ydb-platform/ydb-embedded-ui/commit/65cdf9ee93277c193cc1ad036b2cb38d2ae15b71))
* **Query:** transfer API calls to a new line ([#499](https://github.com/ydb-platform/ydb-embedded-ui/issues/499)) ([de3d540](https://github.com/ydb-platform/ydb-embedded-ui/commit/de3d5404310f32ba05598bb99a1afb1b65ab45a1))
* **SchemaTree:** transfer Show Preview to SchemaTree ([#505](https://github.com/ydb-platform/ydb-embedded-ui/issues/505)) ([46220c4](https://github.com/ydb-platform/ydb-embedded-ui/commit/46220c4b2cd111acf12712b4693744c52aaf7231))

## [4.13.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v4.12.0...v4.13.0) (2023-08-04)


### Features

* info and summary tabs for external objects ([#493](https://github.com/ydb-platform/ydb-embedded-ui/issues/493)) ([88d9041](https://github.com/ydb-platform/ydb-embedded-ui/commit/88d9041f080f13046aeaf55765609dbc13b87285))


### Bug Fixes

* **SchemaTree:** add actions to external objects ([#497](https://github.com/ydb-platform/ydb-embedded-ui/issues/497)) ([5029579](https://github.com/ydb-platform/ydb-embedded-ui/commit/5029579796dd5fb985005f39e9ef8daf142366d0))
* **SchemaTree:** set required query mode for tree actions ([#491](https://github.com/ydb-platform/ydb-embedded-ui/issues/491)) ([ccd1eda](https://github.com/ydb-platform/ydb-embedded-ui/commit/ccd1edac0d84357cd605c9d131c99890449d8bd8))

## [4.12.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v4.11.1...v4.12.0) (2023-08-02)


### Features

* **Query:** add explanation to the query method selector ([#492](https://github.com/ydb-platform/ydb-embedded-ui/issues/492)) ([ce6407c](https://github.com/ydb-platform/ydb-embedded-ui/commit/ce6407c254e9498d5b3bce60298905ea72621766))


### Bug Fixes

* fix tablet size ([#490](https://github.com/ydb-platform/ydb-embedded-ui/issues/490)) ([5a9b9d9](https://github.com/ydb-platform/ydb-embedded-ui/commit/5a9b9d955a882b1191502f5bac8eff5cf8638a52))
* **Search:** add minimum width to Search ([#494](https://github.com/ydb-platform/ydb-embedded-ui/issues/494)) ([2add1dc](https://github.com/ydb-platform/ydb-embedded-ui/commit/2add1dcb3c8a76297ab35600e6d8a8772a411b1d))

## [4.11.1](https://github.com/ydb-platform/ydb-embedded-ui/compare/v4.11.0...v4.11.1) (2023-07-27)


### Bug Fixes

* **Issues:** fix types ([#488](https://github.com/ydb-platform/ydb-embedded-ui/issues/488)) ([e2fe731](https://github.com/ydb-platform/ydb-embedded-ui/commit/e2fe731ae23db6703f21179668582d5657de9550))

## [4.11.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v4.10.1...v4.11.0) (2023-07-27)


### Features

* support external objects in schema tree ([#485](https://github.com/ydb-platform/ydb-embedded-ui/issues/485)) ([cf96f9a](https://github.com/ydb-platform/ydb-embedded-ui/commit/cf96f9af02db1352f3990f21f8a84c1282229517))


### Bug Fixes

* **ClusterInfo:** change cluster default name ([#478](https://github.com/ydb-platform/ydb-embedded-ui/issues/478)) ([398df6e](https://github.com/ydb-platform/ydb-embedded-ui/commit/398df6e3a5778c245653f61b41ba2e1bd0ea3a51))
* fix copy schema action ([#483](https://github.com/ydb-platform/ydb-embedded-ui/issues/483)) ([f6b01c3](https://github.com/ydb-platform/ydb-embedded-ui/commit/f6b01c3cc2808337d5597f990f65ff3e7c010b05))
* **Nodes:** support v2 compute ([#476](https://github.com/ydb-platform/ydb-embedded-ui/issues/476)) ([696d43a](https://github.com/ydb-platform/ydb-embedded-ui/commit/696d43a04109c7fc68986e036e66767593af8d00))
* **ObjectSummary:** fix issue on object change with active schema tab ([#482](https://github.com/ydb-platform/ydb-embedded-ui/issues/482)) ([b50db5f](https://github.com/ydb-platform/ydb-embedded-ui/commit/b50db5ff742c5c7fc27e292309831b937e5d40bd))
* **ObjectSummary:** fix wrong tree alignment bug ([#486](https://github.com/ydb-platform/ydb-embedded-ui/issues/486)) ([e8bfe99](https://github.com/ydb-platform/ydb-embedded-ui/commit/e8bfe99657870c735a41d24febaa907ac1383479))
* **Query:** process null issues error ([#480](https://github.com/ydb-platform/ydb-embedded-ui/issues/480)) ([4c4e684](https://github.com/ydb-platform/ydb-embedded-ui/commit/4c4e6845e539296ecbdefa930bc63d3321f277dc))

## [4.10.1](https://github.com/ydb-platform/ydb-embedded-ui/compare/v4.10.0...v4.10.1) (2023-07-14)


### Bug Fixes

* apply design fixes ([#475](https://github.com/ydb-platform/ydb-embedded-ui/issues/475)) ([5e7c9ca](https://github.com/ydb-platform/ydb-embedded-ui/commit/5e7c9caa9f54094a3eb6448d92d43242d3e738dd))
* **AsideNavigation:** replace query icon ([#466](https://github.com/ydb-platform/ydb-embedded-ui/issues/466)) ([4495eb2](https://github.com/ydb-platform/ydb-embedded-ui/commit/4495eb2634e48feda677c03591b92393ad28981e))
* **ClusterInfo:** add Databases field ([#474](https://github.com/ydb-platform/ydb-embedded-ui/issues/474)) ([28a9936](https://github.com/ydb-platform/ydb-embedded-ui/commit/28a99364bf5e916381a54a59d4d3f979b35f6eff))
* **Cluster:** make global scroll ([#470](https://github.com/ydb-platform/ydb-embedded-ui/issues/470)) ([30f8bc5](https://github.com/ydb-platform/ydb-embedded-ui/commit/30f8bc5ce52fceda076d278b1464d413e899ae21))
* **Cluster:** remove tabs icons and numbers ([#473](https://github.com/ydb-platform/ydb-embedded-ui/issues/473)) ([d2e43d4](https://github.com/ydb-platform/ydb-embedded-ui/commit/d2e43d41759b085f34b7f29f52f3aba60cd0588f))
* **Query:** rename New Query tab to Query ([#467](https://github.com/ydb-platform/ydb-embedded-ui/issues/467)) ([c3f5585](https://github.com/ydb-platform/ydb-embedded-ui/commit/c3f5585562a204ef0831d0c45766b17c3dc72f82))
* **TableIndexInfo:** format DataSize ([#468](https://github.com/ydb-platform/ydb-embedded-ui/issues/468)) ([a189b8c](https://github.com/ydb-platform/ydb-embedded-ui/commit/a189b8cf9610f6b1b7b5f4c01896eda5f8347ebf))

## [4.10.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v4.9.0...v4.10.0) (2023-07-07)


### Bug Fixes

* **AsideNavigation:** swap icons ([#465](https://github.com/ydb-platform/ydb-embedded-ui/issues/465)) ([13bc92a](https://github.com/ydb-platform/ydb-embedded-ui/commit/13bc92a0150ee8d809b3811b528f5d31f4999815))
* move sendQuery timeout to request query ([#464](https://github.com/ydb-platform/ydb-embedded-ui/issues/464)) ([6323038](https://github.com/ydb-platform/ydb-embedded-ui/commit/6323038b9e327a9e348812b43514008e9d07640c))
* **QueryEditor:** do not reset input on empty savedPath ([#451](https://github.com/ydb-platform/ydb-embedded-ui/issues/451)) ([7f98e44](https://github.com/ydb-platform/ydb-embedded-ui/commit/7f98e44834b54bfc1398bb418909fae21e22a3dc))
* show 5 digits size in table info ([#461](https://github.com/ydb-platform/ydb-embedded-ui/issues/461)) ([8c4ecc4](https://github.com/ydb-platform/ydb-embedded-ui/commit/8c4ecc41ed41cad34debaa6ff7f39f1f10f8d974))
* **TenantOverview:** add copy button to tenant name ([#459](https://github.com/ydb-platform/ydb-embedded-ui/issues/459)) ([2d8b380](https://github.com/ydb-platform/ydb-embedded-ui/commit/2d8b38049a038fb889e82d0135c026462107a124))
* **UsageFilter:** fix bar flashes ([#457](https://github.com/ydb-platform/ydb-embedded-ui/issues/457)) ([ae1965e](https://github.com/ydb-platform/ydb-embedded-ui/commit/ae1965ec894c7d012f0ebfc5949b73d4499b390e))

## [4.9.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v4.8.2...v4.9.0) (2023-06-30)


### Features

* **QueryEditor:** remove old controls, update setting ([#445](https://github.com/ydb-platform/ydb-embedded-ui/issues/445)) ([75efd44](https://github.com/ydb-platform/ydb-embedded-ui/commit/75efd444c8b8ba5213117ec9c33f6b9664855a2c))


### Bug Fixes

* **QueryEditor:** color last used query action, run on command ([#436](https://github.com/ydb-platform/ydb-embedded-ui/issues/436)) ([c4d3bb8](https://github.com/ydb-platform/ydb-embedded-ui/commit/c4d3bb81bc1cea8ec3fe2e5e7e18c997d94f5714))
* **QueryEditor:** rename query modes ([#449](https://github.com/ydb-platform/ydb-embedded-ui/issues/449)) ([c93c9c1](https://github.com/ydb-platform/ydb-embedded-ui/commit/c93c9c17ba26e01c596009657cac02ecc9cc9ab0))
* **StorageNodes:** sort by uptime ([#447](https://github.com/ydb-platform/ydb-embedded-ui/issues/447)) ([283cb81](https://github.com/ydb-platform/ydb-embedded-ui/commit/283cb81b3f4711ddc2bb991615729a9bda7e893c))
* **Storage:** remove visible entities filter ([#448](https://github.com/ydb-platform/ydb-embedded-ui/issues/448)) ([b4d9489](https://github.com/ydb-platform/ydb-embedded-ui/commit/b4d948965cd349a54fe833a6b81ea3b087782735))

## [4.8.2](https://github.com/ydb-platform/ydb-embedded-ui/compare/v4.8.1...v4.8.2) (2023-06-27)


### Bug Fixes

* **breadcrumbs:** update tenant and tablet params ([#443](https://github.com/ydb-platform/ydb-embedded-ui/issues/443)) ([b0d31ac](https://github.com/ydb-platform/ydb-embedded-ui/commit/b0d31acce6d6e97d759180c885e6aea3b762a91c))

## [4.8.1](https://github.com/ydb-platform/ydb-embedded-ui/compare/v4.8.0...v4.8.1) (2023-06-26)


### Bug Fixes

* **Tenants:** fix tenant link ([#439](https://github.com/ydb-platform/ydb-embedded-ui/issues/439)) ([432c621](https://github.com/ydb-platform/ydb-embedded-ui/commit/432c621eb2fb2ffd5a747299af930236d5cc06f7))

## [4.8.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v4.7.0...v4.8.0) (2023-06-26)


### Features

* **Tenant:** transform general tabs into left navigation items ([#431](https://github.com/ydb-platform/ydb-embedded-ui/issues/431)) ([7117b96](https://github.com/ydb-platform/ydb-embedded-ui/commit/7117b9622d5f6469dcc2bcc1c0d5cb71d4f94c0b))

## [4.7.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v4.6.0...v4.7.0) (2023-06-23)


### Features

* **QueryEditor:** transform history and saved to tabs ([#427](https://github.com/ydb-platform/ydb-embedded-ui/issues/427)) ([6378ca7](https://github.com/ydb-platform/ydb-embedded-ui/commit/6378ca7013239b33e55c1f88fdde7cab3a102df6))
* update breadcrumbs ([#432](https://github.com/ydb-platform/ydb-embedded-ui/issues/432)) ([e583a03](https://github.com/ydb-platform/ydb-embedded-ui/commit/e583a03fe0d77698f29c924e611133f015c3f7ad))


### Bug Fixes

* **Cluster:** add icons to tabs ([#430](https://github.com/ydb-platform/ydb-embedded-ui/issues/430)) ([e9e649f](https://github.com/ydb-platform/ydb-embedded-ui/commit/e9e649f614691e44172c9b93dd3119066c145413))
* **ClusterInfo:** hide by default ([#435](https://github.com/ydb-platform/ydb-embedded-ui/issues/435)) ([ef2b353](https://github.com/ydb-platform/ydb-embedded-ui/commit/ef2b3535f2c6324a34c4386680f5050655a04eb4))
* **Cluster:** use counter from uikit for tabs ([#428](https://github.com/ydb-platform/ydb-embedded-ui/issues/428)) ([19ca3bd](https://github.com/ydb-platform/ydb-embedded-ui/commit/19ca3bd14b15bdab1a9621939ddceee6d23b08ac))
* **DetailedOverview:** prevent tenant info scroll on overflow ([#434](https://github.com/ydb-platform/ydb-embedded-ui/issues/434)) ([8ed6076](https://github.com/ydb-platform/ydb-embedded-ui/commit/8ed60760d54913d05f39d35d00a34c8b1d7d9738))
* rename Internal Viewer to Developer UI ([#423](https://github.com/ydb-platform/ydb-embedded-ui/issues/423)) ([3eb21f3](https://github.com/ydb-platform/ydb-embedded-ui/commit/3eb21f35a230cc591f02ef9b195f99031f832e8a))
* **Storage:** update columns ([#437](https://github.com/ydb-platform/ydb-embedded-ui/issues/437)) ([264fbc9](https://github.com/ydb-platform/ydb-embedded-ui/commit/264fbc984cd9ef1467110d3e2f5ed9b29a526c2b))
* **Tablet:** clear tablet data on unmount ([#425](https://github.com/ydb-platform/ydb-embedded-ui/issues/425)) ([5d308cd](https://github.com/ydb-platform/ydb-embedded-ui/commit/5d308cdded342d7a40cbc6a91431d3f286c39b8a))
* **TabletsStatistic:** use tenant backend ([#429](https://github.com/ydb-platform/ydb-embedded-ui/issues/429)) ([d290684](https://github.com/ydb-platform/ydb-embedded-ui/commit/d290684ba08aec8b66c0492ba571a5337b5b896c))

## [4.6.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v4.5.2...v4.6.0) (2023-06-13)


### Features

* **QueryEditor:** add data and query modes ([#422](https://github.com/ydb-platform/ydb-embedded-ui/issues/422)) ([c142f03](https://github.com/ydb-platform/ydb-embedded-ui/commit/c142f03e9caeab4dcf1d34b3988e949a94213932))
* rework navigation, update breadcrumbs ([#418](https://github.com/ydb-platform/ydb-embedded-ui/issues/418)) ([2d807d6](https://github.com/ydb-platform/ydb-embedded-ui/commit/2d807d6a52e13edcf2a7e1591672224339d91949))


### Bug Fixes

* **Diagnostics:** remove unneded tenantInfo fetch ([#420](https://github.com/ydb-platform/ydb-embedded-ui/issues/420)) ([ccaafe4](https://github.com/ydb-platform/ydb-embedded-ui/commit/ccaafe4ec9346ee1ec2ebd2a62600274f2175bfb))

## [4.5.2](https://github.com/ydb-platform/ydb-embedded-ui/compare/v4.5.1...v4.5.2) (2023-06-06)


### Bug Fixes

* **Versions:** enable table dynamic render ([#416](https://github.com/ydb-platform/ydb-embedded-ui/issues/416)) ([3c877ea](https://github.com/ydb-platform/ydb-embedded-ui/commit/3c877ea88a0c4036213b38099676f473cf3ac2d6))

## [4.5.1](https://github.com/ydb-platform/ydb-embedded-ui/compare/v4.5.0...v4.5.1) (2023-06-02)


### Bug Fixes

* **Tablet:** fetch data on action finish ([#405](https://github.com/ydb-platform/ydb-embedded-ui/issues/405)) ([f1d71c5](https://github.com/ydb-platform/ydb-embedded-ui/commit/f1d71c5af330a0a13246f8d87433e6bba1d3509a))

## [4.5.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v4.4.2...v4.5.0) (2023-06-01)


### Features

* **ClusterInfo:** update versions bar, rework DC and Tablets fields ([#407](https://github.com/ydb-platform/ydb-embedded-ui/issues/407)) ([4824f0d](https://github.com/ydb-platform/ydb-embedded-ui/commit/4824f0d2be9d7bec3641302c88b39a3a87f37c18))

## [4.4.2](https://github.com/ydb-platform/ydb-embedded-ui/compare/v4.4.1...v4.4.2) (2023-05-29)


### Bug Fixes

* **Partitions:** fix offsets calculation ([#402](https://github.com/ydb-platform/ydb-embedded-ui/issues/402)) ([fd4741f](https://github.com/ydb-platform/ydb-embedded-ui/commit/fd4741f8761aa6aa9ec31681522c4d261a83273f))

## [4.4.1](https://github.com/ydb-platform/ydb-embedded-ui/compare/v4.4.0...v4.4.1) (2023-05-25)


### Bug Fixes

* **Nodes:** fix endpoint setting ([#397](https://github.com/ydb-platform/ydb-embedded-ui/issues/397)) ([4aea8a2](https://github.com/ydb-platform/ydb-embedded-ui/commit/4aea8a2597909338e31ac51577989a4d82ec93cf))

## [4.4.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v4.3.0...v4.4.0) (2023-05-25)


### Features

* add Versions ([#394](https://github.com/ydb-platform/ydb-embedded-ui/issues/394)) ([d5abb58](https://github.com/ydb-platform/ydb-embedded-ui/commit/d5abb586a127135c5756a3aa5076060c0dce3fba))
* remove unsupported pages ([b2bc3b2](https://github.com/ydb-platform/ydb-embedded-ui/commit/b2bc3b22029679769bb0de73f2c33827028de8a8))


### Bug Fixes

* **ClusterInfo:** do not show response error on cancelled requests ([83501b5](https://github.com/ydb-platform/ydb-embedded-ui/commit/83501b50f0c266ba654858767ca89a2a3fa891ed))
* **Cluster:** remove padding from cluster page ([8138823](https://github.com/ydb-platform/ydb-embedded-ui/commit/8138823a9d5d3dbd1f086fb0bb23265d7faa8025))
* **Partitions:** fix columns titles ([4fe21a0](https://github.com/ydb-platform/ydb-embedded-ui/commit/4fe21a0dc149c7bca0611c74990756fbdc5fb273))
* **Partitions:** update Select empty value ([a7df6d1](https://github.com/ydb-platform/ydb-embedded-ui/commit/a7df6d1c86224a4534fac048cebc61b6f5a78fde))
* **UserSettings:** separate Setting, enable additional settings ([#396](https://github.com/ydb-platform/ydb-embedded-ui/issues/396)) ([e8a17a1](https://github.com/ydb-platform/ydb-embedded-ui/commit/e8a17a160c82212a181b1ef4e3b9f223db29907e))

## [4.3.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v4.2.1...v4.3.0) (2023-05-18)


### Features

* **Partitions:** display partitions for topic without consumers ([0843a49](https://github.com/ydb-platform/ydb-embedded-ui/commit/0843a49c46cb6765b376832a847c3ac0ce8b6b85))


### Bug Fixes

* **Tablet:** update state to color mapping ([7ccc8c7](https://github.com/ydb-platform/ydb-embedded-ui/commit/7ccc8c79225cd311a7a3674150335b58a94f293e))

## [4.2.1](https://github.com/ydb-platform/ydb-embedded-ui/compare/v4.2.0...v4.2.1) (2023-05-18)


### Bug Fixes

* export toaster ([b5d12c0](https://github.com/ydb-platform/ydb-embedded-ui/commit/b5d12c0aa39ea3877a9b74071e3124f89a309ca3))

## [4.2.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v4.1.0...v4.2.0) (2023-05-16)


### Features

* **Tablet:** display node fqdn in table ([4d8099a](https://github.com/ydb-platform/ydb-embedded-ui/commit/4d8099a454f34fc76886b26ca948895171c57ab8))


### Bug Fixes

* **api:** change nulls to empty objects ([0ab14e8](https://github.com/ydb-platform/ydb-embedded-ui/commit/0ab14e883a47aeac2f2bab437f2214a32ccb1c9b))
* display storage pool in VDisks popups ([5b5dd8a](https://github.com/ydb-platform/ydb-embedded-ui/commit/5b5dd8a4e6cb4bcc1ead78a7c06d2e80a81424cc))
* fix Select label and values align ([f796730](https://github.com/ydb-platform/ydb-embedded-ui/commit/f7967309fe4a042e7637de212f33b1ebfc6877fc))
* **Overview:** partitioning by size disabled for 0 SizeToSpit ([1028e7d](https://github.com/ydb-platform/ydb-embedded-ui/commit/1028e7d8d3566f5f5e6b2ebe04112ef135d7b55e))
* **Schema:** display NotNull columns ([d61eaa4](https://github.com/ydb-platform/ydb-embedded-ui/commit/d61eaa4ccff357c1e9ca6efde855ec46be24a314))

## [4.1.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v4.0.0...v4.1.0) (2023-05-10)


### Features

* **Navigation:** remove legacy navigation setting support ([8544f11](https://github.com/ydb-platform/ydb-embedded-ui/commit/8544f114255ba44834d38cd9e709450c49e4a96a))


### Bug Fixes

* disable link and popover for unavailable nodes ([990a9fa](https://github.com/ydb-platform/ydb-embedded-ui/commit/990a9fa42a7133a6c40d07e11c3518240e18b4a9))

## [4.0.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v3.5.0...v4.0.0) (2023-04-28)


### ⚠ BREAKING CHANGES

* app no longer parses query responses from older ydb versions
* v0.1 explain plans are no longer rendered

### Features

* enable explain-script parsing, remove deprecated code ([5c6e9a2](https://github.com/ydb-platform/ydb-embedded-ui/commit/5c6e9a21026ea9eb3e32650e6fdda89c7900e7e6))
* **QueryEditor:** add explain query modes ([39ad943](https://github.com/ydb-platform/ydb-embedded-ui/commit/39ad9434c1622e22901e6cc1af1568e0edf6b434))
* **QueryEditor:** display query duration ([967f102](https://github.com/ydb-platform/ydb-embedded-ui/commit/967f10296d2362709654172ed7318509286efc78))
* remove support for explain v0.1 ([c8741a6](https://github.com/ydb-platform/ydb-embedded-ui/commit/c8741a69b82053185a07c7ba563455d4f28ecdce))


### Bug Fixes

* **query:** correctly process NetworkError on actions failure ([cf5bd6c](https://github.com/ydb-platform/ydb-embedded-ui/commit/cf5bd6c5c4c2972fec93b2dc9135c92c639fa5f9))
* **QueryExplain:** do not request ast when missing ([54cf151](https://github.com/ydb-platform/ydb-embedded-ui/commit/54cf151452e17256173736450f5727085ea591ff))
* **QueryExplain:** request AST if it is empty ([d028b4e](https://github.com/ydb-platform/ydb-embedded-ui/commit/d028b4ed08a98281baff81683204f1cbc1c20c37))

## [3.5.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v3.4.5...v3.5.0) (2023-04-18)


### Features

* **TableInfo:** extend Table and ColumnTable info ([89e54aa](https://github.com/ydb-platform/ydb-embedded-ui/commit/89e54aa97d7bcbabfd5100daeb1dc0c03608e86e))
* **TopQueries:** add columns ([b49b98d](https://github.com/ydb-platform/ydb-embedded-ui/commit/b49b98db2da08c355b23f4a33bf05247530543db))


### Bug Fixes

* **settings:** use system theme by default ([726c9cb](https://github.com/ydb-platform/ydb-embedded-ui/commit/726c9cb14d7f87cc9248340d1ebebfc8bf0d0384))
* **Storage:** fix incorrect usage on zero available space ([2704cd7](https://github.com/ydb-platform/ydb-embedded-ui/commit/2704cd7c696d337cc8e3af68941cf444f8dfae81))
* **TableInfo:** add default format for FollowerGroup fields ([961334a](https://github.com/ydb-platform/ydb-embedded-ui/commit/961334aabe89672994f0f3440e20602e180b3394))
* **Tablet:** fix dialog type enum ([c477042](https://github.com/ydb-platform/ydb-embedded-ui/commit/c477042cacc2e777cae4bd6981381a8042c603ed))
* **TopQueries:** enable go back to TopQueries from Query tab ([bbdfe72](https://github.com/ydb-platform/ydb-embedded-ui/commit/bbdfe726c9081f01422dca787b83399ea44b3956))
* **TopShards:** fix table crash on undefined values ([604e99a](https://github.com/ydb-platform/ydb-embedded-ui/commit/604e99a9427021c61ceb8ea366e316e629032b84))
* **TruncatedQuery:** wrap message ([f41b7ff](https://github.com/ydb-platform/ydb-embedded-ui/commit/f41b7ff33ac0145446ca89aab031036247f3ddf8))

## [3.4.5](https://github.com/ydb-platform/ydb-embedded-ui/compare/v3.4.4...v3.4.5) (2023-03-30)


### Bug Fixes

* **Consumers:** fix typo ([aaa9dbd](https://github.com/ydb-platform/ydb-embedded-ui/commit/aaa9dbda1f28702917793a61bae2813f6ef018bb))
* **PDisk:** add display block to content ([130dab2](https://github.com/ydb-platform/ydb-embedded-ui/commit/130dab20ffdc9da77225c94a6e6064f0308a1c2a))
* **Storage:** get nodes hosts from /nodelist ([cc82dd9](https://github.com/ydb-platform/ydb-embedded-ui/commit/cc82dd93808133b0d1dcd21b31ee3744df4f7383))
* **StorageNodes:** make fqdn similar to nodes page ([344298a](https://github.com/ydb-platform/ydb-embedded-ui/commit/344298a9a29380f1068b002fa304cdcc221ce0d4))
* **TopicInfo:** do not display /s when speed is undefined ([2d41832](https://github.com/ydb-platform/ydb-embedded-ui/commit/2d4183247ec33acdfa45be72a93f0dbd93b716e0))
* **TopicStats:** use prepared stats, update fields ([a614a8c](https://github.com/ydb-platform/ydb-embedded-ui/commit/a614a8caa2744b844d97f23f25e5385387367d6b))

## [3.4.4](https://github.com/ydb-platform/ydb-embedded-ui/compare/v3.4.3...v3.4.4) (2023-03-22)


### Bug Fixes

* **Diagnostics:** display nodes tab for not db entities ([a542dbc](https://github.com/ydb-platform/ydb-embedded-ui/commit/a542dbc23d01138a5c1a4126cfc1836a1543b68c))

## [3.4.3](https://github.com/ydb-platform/ydb-embedded-ui/compare/v3.4.2...v3.4.3) (2023-03-17)


### Bug Fixes

* add opacity to unavailable nodes ([8b82c78](https://github.com/ydb-platform/ydb-embedded-ui/commit/8b82c78f0b6bed536ca23c63b78b141b29afc4a8))
* **Tablet:** add error check ([49f13cf](https://github.com/ydb-platform/ydb-embedded-ui/commit/49f13cf0cff2d6dad59b8f6a4c2885966bf3450a))
* **VDisk:** fix typo ([1528d03](https://github.com/ydb-platform/ydb-embedded-ui/commit/1528d03531f482e438e0bdb6c761be236822fc27))

## [3.4.2](https://github.com/ydb-platform/ydb-embedded-ui/compare/v3.4.1...v3.4.2) (2023-03-03)


### Bug Fixes

* **Partitions:** add search to consumers filter ([95e4462](https://github.com/ydb-platform/ydb-embedded-ui/commit/95e446295cb2b2729daf0d0ef719e37c7c8e0d3c))
* **Partitions:** fix error on wrong consumer in query string ([44269fa](https://github.com/ydb-platform/ydb-embedded-ui/commit/44269fa9240fe31c9ef69e061c20d58b2b55fae3))
* **PDisk:** display vdisks donors ([8b39b01](https://github.com/ydb-platform/ydb-embedded-ui/commit/8b39b01e8bf62624e9e12ac0a329fda5d03cc8df))

## [3.4.1](https://github.com/ydb-platform/ydb-embedded-ui/compare/v3.4.0...v3.4.1) (2023-03-01)


### Bug Fixes

* **Consumers:** enable navigation to Partitions tab ([fa79081](https://github.com/ydb-platform/ydb-embedded-ui/commit/fa7908124bc4392e272aa829fd4e5c1639fcf209))
* **Consumers:** update topic stats values align ([f2af851](https://github.com/ydb-platform/ydb-embedded-ui/commit/f2af851208a640ef9aa392fd7176eb579a2401db))
* **TopShards:** keep state on request cancel ([1bd4f65](https://github.com/ydb-platform/ydb-embedded-ui/commit/1bd4f65dd047b42f8edf9e4bb41c722f30220d77))

## [3.4.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v3.3.4...v3.4.0) (2023-02-17)


### Features

* **Diagnostics:** add Partitions tab ([914702b](https://github.com/ydb-platform/ydb-embedded-ui/commit/914702be7e8aea28fcdc9f2ddf1cb7356995146a))
* **Diagnostics:** rework Consumers tab ([0dae9d8](https://github.com/ydb-platform/ydb-embedded-ui/commit/0dae9d84c254d556db2a0d18345fdc10c152172a))


### Bug Fixes

* add read and lag images ([a3f0648](https://github.com/ydb-platform/ydb-embedded-ui/commit/a3f0648fc4f23c2ac2c9e73c4078bf5f06d1a57e))
* add reducer for consumer ([4ab65e3](https://github.com/ydb-platform/ydb-embedded-ui/commit/4ab65e3fb3dd4f29b4757473275ba84bec0f5411))
* add SpeedMultiMeter component ([39acbf1](https://github.com/ydb-platform/ydb-embedded-ui/commit/39acbf1a1e234f36a090b29935872e694e1525c0))
* **ResponseError:** make error prop optional ([f706e94](https://github.com/ydb-platform/ydb-embedded-ui/commit/f706e940e51e62841e18338775b01183831761e1))
* **Storage:** display not full donors ([13f4b9f](https://github.com/ydb-platform/ydb-embedded-ui/commit/13f4b9fe9f796e8ef6fee094f7b5bc6056e2833b))
* **Topic:** use SpeedMultiMeter and utils functions ([3e0293c](https://github.com/ydb-platform/ydb-embedded-ui/commit/3e0293cc5cf69c2dee5b6c4cdcf053829960dac5))
* **utils:** add formatBytesCustom function ([2f18c22](https://github.com/ydb-platform/ydb-embedded-ui/commit/2f18c2233b37b666e16327af0ca8e20bccf01de6))

## [3.3.4](https://github.com/ydb-platform/ydb-embedded-ui/compare/v3.3.3...v3.3.4) (2023-02-16)


### Bug Fixes

* **OverloadedShards:** rename to top shards ([ffa4f27](https://github.com/ydb-platform/ydb-embedded-ui/commit/ffa4f27f2cf0a5e12b2800c81bf61b1d3c25912c))
* **StorageGroups:** display Erasure ([4a7ebc0](https://github.com/ydb-platform/ydb-embedded-ui/commit/4a7ebc08b87fe75af83df70a38ebd486d64d6d4e))
* **TopShards:** switch between history and immediate data ([eeb9bb0](https://github.com/ydb-platform/ydb-embedded-ui/commit/eeb9bb0911b9e889b633558c9d3c13f986f72bfe))

## [3.3.3](https://github.com/ydb-platform/ydb-embedded-ui/compare/v3.3.2...v3.3.3) (2023-02-08)


### Bug Fixes

* **Auth:** add a step in history for auth form ([c72d06e](https://github.com/ydb-platform/ydb-embedded-ui/commit/c72d06ecacdba47cac59bd705c1185e1ddf0b20d))
* format dates with date-utils ([948598b](https://github.com/ydb-platform/ydb-embedded-ui/commit/948598b83c9bdd09268d128e15a42d5a6e0c15cc))
* **InfoViewer:** add prop renderEmptyState ([44fe28f](https://github.com/ydb-platform/ydb-embedded-ui/commit/44fe28f72ea299b3b5d9b5a33a0a0130d471f7dd))
* minor fixes in Nodes and Tenants tables ([8dca43a](https://github.com/ydb-platform/ydb-embedded-ui/commit/8dca43a482b0da31dbc618875b416dcfcedac036))
* **OverloadedShards:** display IntervalEnd ([c7cbd72](https://github.com/ydb-platform/ydb-embedded-ui/commit/c7cbd7215eaf60601941410acb13ffb25d151eb9))
* **Overview:** display error statusText on schema error ([99b030f](https://github.com/ydb-platform/ydb-embedded-ui/commit/99b030f90e6044e98a151e5128603835c84e1b4e))
* **PDisk:** calculate severity based on usage ([64c6890](https://github.com/ydb-platform/ydb-embedded-ui/commit/64c6890ac6d5a77aef73da7dfc7f1eaff8a72441))
* **QueryEditor:** make client request timeout 9 min ([44528a8](https://github.com/ydb-platform/ydb-embedded-ui/commit/44528a865b039003cda4c7b1b1367840da015d09))
* **QueryEditor:** result status for aborted connection ([4b0d84b](https://github.com/ydb-platform/ydb-embedded-ui/commit/4b0d84b550deb41a140d4a3d215e52084507a558))
* **QueryResult:** output client error messages ([deef610](https://github.com/ydb-platform/ydb-embedded-ui/commit/deef6103f4d08825837520cab9e8ae5b8c7fd496))
* **Storage:** replace hasOwn to hasOwnProperty ([2452310](https://github.com/ydb-platform/ydb-embedded-ui/commit/2452310ce8e953d7a9ee4bbaa2bd466396aa0131))
* **TopQueries:** display IntervalEnd ([e5b2b07](https://github.com/ydb-platform/ydb-embedded-ui/commit/e5b2b07cf1e686c20817dcdc1ae32e0c8912a21a))

## [3.3.2](https://github.com/ydb-platform/ydb-embedded-ui/compare/v3.3.1...v3.3.2) (2023-01-31)


### Bug Fixes

* **QueryEditor:** collapse bottom panel if empty ([566db3b](https://github.com/ydb-platform/ydb-embedded-ui/commit/566db3b15c4393555071f058c88ad36b4073cc2d))
* **VDisk:** use pdiskid field for link ([5ee0705](https://github.com/ydb-platform/ydb-embedded-ui/commit/5ee0705416aa31be9bee4be0776ecb8a61d3e82c))

## [3.3.1](https://github.com/ydb-platform/ydb-embedded-ui/compare/v3.3.0...v3.3.1) (2023-01-31)


### Bug Fixes

* **UserSettings:** reword nodes setting and add popup ([2fda2b8](https://github.com/ydb-platform/ydb-embedded-ui/commit/2fda2b815b921a8163f80527c45f788172df4ba8))

## [3.3.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v3.2.3...v3.3.0) (2023-01-30)


### Features

* **Nodes:** use /viewer/json/nodes endpoint ([226cc70](https://github.com/ydb-platform/ydb-embedded-ui/commit/226cc70dcb89262890856b4d0cb03eac0675256d))
* **Overview:** display topic stats for topics and streams ([08e9fe0](https://github.com/ydb-platform/ydb-embedded-ui/commit/08e9fe0ee379715229474322a03ec668e26bdb9b))
* **Storage:** display vdisks over pdisks ([bb5d1fa](https://github.com/ydb-platform/ydb-embedded-ui/commit/bb5d1fa5ae2953ca30b13df45340b7a1a63056cb))


### Bug Fixes

* add duration formatter ([e325d98](https://github.com/ydb-platform/ydb-embedded-ui/commit/e325d98845d29dea208debdfcb88d330c1d6daee))
* add protobuf time formatters ([c74cd9d](https://github.com/ydb-platform/ydb-embedded-ui/commit/c74cd9d0949674414ba2c9754e3dcc5c2be622a5))
* add verticalBars component ([053ffa8](https://github.com/ydb-platform/ydb-embedded-ui/commit/053ffa8fd460f89f4296a96fcf46a9267ac4cae3))
* **PDisk:** grey color for unknown state ([54f7e15](https://github.com/ydb-platform/ydb-embedded-ui/commit/54f7e159aaddd932ccecddfb10265ee596fed1e2))
* **Storage:** request only static nodes ([e91e136](https://github.com/ydb-platform/ydb-embedded-ui/commit/e91e136d7c72bea694c7a282c83d577cc60e5386))
* **Topic:** add reducer for describe_topic ([7c61dc9](https://github.com/ydb-platform/ydb-embedded-ui/commit/7c61dc906452df2e1a77a2ff602916f6ea785df5))
* update PDisks and VDisks tests ([3bf660e](https://github.com/ydb-platform/ydb-embedded-ui/commit/3bf660e41d92e1a32444872c5fb9d47209bef8b5))

## [3.2.3](https://github.com/ydb-platform/ydb-embedded-ui/compare/v3.2.2...v3.2.3) (2023-01-16)


### Bug Fixes

* fix crash on invalid search query ([4d6f551](https://github.com/ydb-platform/ydb-embedded-ui/commit/4d6f551fa4348a05ca3d8d2d6bd8b52ccb6310ee))

## [3.2.2](https://github.com/ydb-platform/ydb-embedded-ui/compare/v3.2.1...v3.2.2) (2023-01-13)


### Bug Fixes

* **Tablets:** fix infinite rerender ([79b3c58](https://github.com/ydb-platform/ydb-embedded-ui/commit/79b3c58fb7c3ff7f123e111189b10f42c5272401))

## [3.2.1](https://github.com/ydb-platform/ydb-embedded-ui/compare/v3.2.0...v3.2.1) (2023-01-12)


### Bug Fixes

* align standard errors to the left ([cce100c](https://github.com/ydb-platform/ydb-embedded-ui/commit/cce100c5df83243df1fb0bc59d84d0d9b33719e6))
* **TabletsFilters:** properly display long data in select options ([ea37d9f](https://github.com/ydb-platform/ydb-embedded-ui/commit/ea37d9fc08245ccdd38a6120dd620f59a528879c))
* **TabletsFilters:** replace constants ([ea948ca](https://github.com/ydb-platform/ydb-embedded-ui/commit/ea948ca86276b5521979105b2ab99546da389e80))
* **TabletsStatistic:** process different tablets state types ([78798de](https://github.com/ydb-platform/ydb-embedded-ui/commit/78798de984bf4f6133515bb1c440e4fe0d15b07e))
* **Tenant:** always display Pools heading ([94baeff](https://github.com/ydb-platform/ydb-embedded-ui/commit/94baeff82f9c2c1aecda7c11c3b090125ba9e4b6))

## [3.2.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v3.1.0...v3.2.0) (2023-01-09)


### Features

* **Nodes:** display rack in table ([3b8cdd5](https://github.com/ydb-platform/ydb-embedded-ui/commit/3b8cdd5b472f98132b2faaa9b71b8911750545a6))
* **StorageNodes:** display datacenter in table ([4507bfd](https://github.com/ydb-platform/ydb-embedded-ui/commit/4507bfde839b0aafa3722828b7528885c6ac8f84))
* **TopQueries:** date range filter ([b9a8e95](https://github.com/ydb-platform/ydb-embedded-ui/commit/b9a8e9504fa68556a724b214ee91b73ec900d37e))
* **TopQueries:** filter by query text ([2c8ea97](https://github.com/ydb-platform/ydb-embedded-ui/commit/2c8ea97dd215ea59165cf05315bc5809cf7fafd7))


### Bug Fixes

* **InfoViewer:** min width for values ([64a4fd4](https://github.com/ydb-platform/ydb-embedded-ui/commit/64a4fd4de16738a9e2fac9cb4fba94eafc938762))
* **Nodes:** open external link in new tab ([b7c3ddd](https://github.com/ydb-platform/ydb-embedded-ui/commit/b7c3ddd1e611f2b61466e3eda51f3341f8407588))
* **TopQueries:** proper table dynamic render type ([9add6ca](https://github.com/ydb-platform/ydb-embedded-ui/commit/9add6ca9fbfe0475caf1586070a800210320cee6))
* **TopShards:** rename to overloaded shards ([d9978bd](https://github.com/ydb-platform/ydb-embedded-ui/commit/d9978bdd84b9a883e4eefcac7f85f856da55d770))
* **UserSettings:** treat invertedDisks settings as string ([ad7742a](https://github.com/ydb-platform/ydb-embedded-ui/commit/ad7742a6bf0be59c2b9cbbf947aaa66f79d748be))

## [3.1.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v3.0.1...v3.1.0) (2022-12-13)


### Features

* **TopShards:** date range filter ([aab4396](https://github.com/ydb-platform/ydb-embedded-ui/commit/aab439600ec28d30799c4a7ef7a9c68fcacc148c))

## [3.0.1](https://github.com/ydb-platform/ydb-embedded-ui/compare/v3.0.0...v3.0.1) (2022-12-12)


### Bug Fixes

* **Overview:** display titles for topic, stream and tableIndex ([2ee7889](https://github.com/ydb-platform/ydb-embedded-ui/commit/2ee788932d4f0a6fbe3e9e0526b8ba50e3103d76))
* **SchemaOverview:** display entity name ([2d28a2a](https://github.com/ydb-platform/ydb-embedded-ui/commit/2d28a2ad30263e31bc3c8b783d4f42af92537624))
* **TenantOverview:** display database type in title ([5f73eed](https://github.com/ydb-platform/ydb-embedded-ui/commit/5f73eed6f9043586885f8e68137d8f31923e8e3b))
* **TopShards:** render a message for empty data ([8cda003](https://github.com/ydb-platform/ydb-embedded-ui/commit/8cda0038396b356b10033b44824933f711e1175e))

## [3.0.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v2.6.0...v3.0.0) (2022-12-05)


### ⚠ BREAKING CHANGES

Updated build config ([11e02c6](https://github.com/ydb-platform/ydb-embedded-ui/commit/11e02c668ef186f058b2ece9d5f1082d0e96e23d))

**Before the change**
- the target dir for the production build was `build/resources`
- `favicon.png` was placed directly in `build`

**After the change**
- the target dir is `build/static`
- `favicon.png` is in `build/static`

This change is intended to simplify build config and make it closer to the default one. Previously there were some custom tweaks after the build, they caused bugs and were hard to maintain. Now the application builds using the default `create-react-app` config.


## [2.6.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v2.5.0...v2.6.0) (2022-12-05)


### Features

* **Describe:** add topic data for CDCStream ([3a289d4](https://github.com/ydb-platform/ydb-embedded-ui/commit/3a289d4f6452e3f2d719c0d508f48b389fd044d7))
* **Diagnostics:** add consumers tab for CdcStream ([22c6efd](https://github.com/ydb-platform/ydb-embedded-ui/commit/22c6efdd39d85ab1585943bc13d88cf03f9bc2ae))
* **Overview:** add topic data for CDCStream ([be80545](https://github.com/ydb-platform/ydb-embedded-ui/commit/be80545df65a03820265875fedd98c6f181af491))


### Bug Fixes

* **Compute:** update data on path change ([1783240](https://github.com/ydb-platform/ydb-embedded-ui/commit/17832403623ae3e718f47aec508c834cd2e3458c))
* **Diagnostics:** render db tabs for not root dbs ([7d46ce2](https://github.com/ydb-platform/ydb-embedded-ui/commit/7d46ce2783a58b1ae6e41cae6592e78f95d61bcc))
* **Healthcheck:** render loader on path change ([ec40f19](https://github.com/ydb-platform/ydb-embedded-ui/commit/ec40f19c0b369de0b8d0658b4a1dd68c5c419c1c))
* **InfoViewer:** allow multiline values ([17755dc](https://github.com/ydb-platform/ydb-embedded-ui/commit/17755dc2eae7b6fc0a56ff70da95679fc590dccb))
* **Network:** update data on path change ([588c53f](https://github.com/ydb-platform/ydb-embedded-ui/commit/588c53f80a81376301216a77d9ead95cdff9812f))
* **SchemaTree:** do not expand childless components ([90468de](https://github.com/ydb-platform/ydb-embedded-ui/commit/90468de74b74e00a66255ba042378c9d7e1cbc27))
* **Storage:** update data on path change ([f5486bc](https://github.com/ydb-platform/ydb-embedded-ui/commit/f5486bcb2838b9e290c566089980533b4d22d035))
* **Tablets:** fix postponed data update on path change ([d474c6c](https://github.com/ydb-platform/ydb-embedded-ui/commit/d474c6cb36597f0c720ef3bb8d0360ec73973e26))
* **TopQueries:** update data on path change ([32d7720](https://github.com/ydb-platform/ydb-embedded-ui/commit/32d77208b8ef09682c41160c60a1a7742b0c6c4c))

## [2.5.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v2.4.4...v2.5.0) (2022-11-25)


### Features

* **Nodes:** add uptime filter ([9bb4f66](https://github.com/ydb-platform/ydb-embedded-ui/commit/9bb4f664df8fadec5b5e612b2adb866c28415efa))
* **NodesViewer:** add uptime filter ([a802442](https://github.com/ydb-platform/ydb-embedded-ui/commit/a8024422a09ff95e55c399d26046f5103cab3f89))
* **Storage:** add nodes uptime filter ([d8cfea1](https://github.com/ydb-platform/ydb-embedded-ui/commit/d8cfea14369e8235d1f7ef86a9a3f34c05efdf5c))


### Bug Fixes

* **Consumers:** add autorefresh to useAutofetcher ([e0da2a1](https://github.com/ydb-platform/ydb-embedded-ui/commit/e0da2a11fcd18cb8ba808a07873a78cbf7191cdc))
* **Consumers:** add loader ([a59f472](https://github.com/ydb-platform/ydb-embedded-ui/commit/a59f472fd7c9347bcde8cc21d4001f999fc88110))
* **QueryExplain:** fix schema rerender on path change ([eb52978](https://github.com/ydb-platform/ydb-embedded-ui/commit/eb529787bf747bb2bf49bae65676011426341a23))
* **Storage:** add message on empty nodes with small uptime ([70959ab](https://github.com/ydb-platform/ydb-embedded-ui/commit/70959ab90bd0f81ebab7712b7d34c0ca80f4dd0b))
* **Storage:** fix uneven PDisks ([0269dba](https://github.com/ydb-platform/ydb-embedded-ui/commit/0269dbab0336ae5b8cbf43e1b52458e932527a66))
* **StorageNodes:** fix message display on not empty data ([bb5fffa](https://github.com/ydb-platform/ydb-embedded-ui/commit/bb5fffa786cde3f680375f8e11e3893c52c4f6da))
* **UsageFilter:** add min-width ([56b2701](https://github.com/ydb-platform/ydb-embedded-ui/commit/56b2701a17420e0322fac0223bce26e18a2f0e47))

## [2.4.4](https://github.com/ydb-platform/ydb-embedded-ui/compare/v2.4.3...v2.4.4) (2022-11-22)


### Bug Fixes

* **api:** update getDescribe and getSchema requests params ([d70ba54](https://github.com/ydb-platform/ydb-embedded-ui/commit/d70ba54b90b9c86a393bd3f7845183114e5afbf1))
* **describe:** cancel concurrent requests ([2f39ad0](https://github.com/ydb-platform/ydb-embedded-ui/commit/2f39ad0f736d44c3749d9523f5024151c51fcf6f))
* **Describe:** render loader on path change ([baf552a](https://github.com/ydb-platform/ydb-embedded-ui/commit/baf552af8bb67046baa36e9115064b4b192cb015))
* **QueryExplain:** fix colors on theme change ([cc0a2d6](https://github.com/ydb-platform/ydb-embedded-ui/commit/cc0a2d67139457748089c6bf1fb1045b0a6b0b93))
* **SchemaTree:** remove unneeded fetches ([c7c0489](https://github.com/ydb-platform/ydb-embedded-ui/commit/c7c048937c5ae9e5e243d6e538aab8c2e2921df5))
* **SchemaTree:** remove unneeded getDescribe ([1146f13](https://github.com/ydb-platform/ydb-embedded-ui/commit/1146f13a7a5a277a292b3789d45a0872dda0c487))
* **Tenant:** make tenant fetch schema only on tenant change ([ccefbff](https://github.com/ydb-platform/ydb-embedded-ui/commit/ccefbffea08fc8f248a3dd1135e82de6db9f0645))

## [2.4.3](https://github.com/ydb-platform/ydb-embedded-ui/compare/v2.4.2...v2.4.3) (2022-11-14)


### Bug Fixes

* fix app crash on ColumnTable path type ([a1aefa8](https://github.com/ydb-platform/ydb-embedded-ui/commit/a1aefa876600b1b459bf3024f0704883431df5a2))
* **schema:** add types for ColumnTable and ColumnStore ([dc13307](https://github.com/ydb-platform/ydb-embedded-ui/commit/dc13307dcea801c05863b7dd5ee19f01aa074c85))

## [2.4.2](https://github.com/ydb-platform/ydb-embedded-ui/compare/v2.4.1...v2.4.2) (2022-11-09)


### Bug Fixes

* **QueryExplain:** apply all node types ([06d26de](https://github.com/ydb-platform/ydb-embedded-ui/commit/06d26def15496f8e2de00d941b39bf6a68382f14))

## [2.4.1](https://github.com/ydb-platform/ydb-embedded-ui/compare/v2.4.0...v2.4.1) (2022-11-01)


### Performance Improvements

* **SchemaTree:** batch preloaded data dispatch ([c9ac514](https://github.com/ydb-platform/ydb-embedded-ui/commit/c9ac514aabf5e9674aae95956604f47ba8a2d257))

## [2.4.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v2.3.0...v2.4.0) (2022-10-27)


### Features

* **Diagnostics:** add consumers tab for topics ([4bb801c](https://github.com/ydb-platform/ydb-embedded-ui/commit/4bb801c0ef19dcda227c59e464b08f5e8f284c38))


### Bug Fixes

* add checks for fetch failure with no errors ([2c55107](https://github.com/ydb-platform/ydb-embedded-ui/commit/2c55107a7b47b3540ed0af66630ff85591f269a1))
* **Nodes:** display access denied on 403 ([7832afe](https://github.com/ydb-platform/ydb-embedded-ui/commit/7832afee601a40fc8b75f83bf0ed18b01c798d71))
* **QueryResult:** fix table display in fullscreen ([98674db](https://github.com/ydb-platform/ydb-embedded-ui/commit/98674db26b5fb09ac0d039a7779ae0c58951adde))
* **QueryResultTable:** make preview display all rows ([0ac83d0](https://github.com/ydb-platform/ydb-embedded-ui/commit/0ac83d0258b0d0d3d2e14c06be096fe5ddce02da))
* **Storage:** display access denied on 403 ([6d20333](https://github.com/ydb-platform/ydb-embedded-ui/commit/6d2033378956a54f05190905b0d537c6bd6c9851))
* **TabletsFilters:** display access denied on 403 ([018be19](https://github.com/ydb-platform/ydb-embedded-ui/commit/018be199602123f1d90e58c0b95545f6accc41fb))

## [2.3.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v2.2.1...v2.3.0) (2022-10-24)


### Features

* **PDisk:** display type on disk progressbar ([00bcbf5](https://github.com/ydb-platform/ydb-embedded-ui/commit/00bcbf5d439ca3bb4834fd5f191c65f0ac62585f))
* **Storage:** display media type for groups ([cdff5e9](https://github.com/ydb-platform/ydb-embedded-ui/commit/cdff5e9882f3f1f8769a3aeaf3e53c05f3ce1c07))
* **Storage:** display shield icon for encrypted groups ([d0a4442](https://github.com/ydb-platform/ydb-embedded-ui/commit/d0a4442dc100c312dcc54afcf685057cc587211d))


### Bug Fixes

* **Diagnostics:** fix tabs reset on page reload ([68d2971](https://github.com/ydb-platform/ydb-embedded-ui/commit/68d297165aea1360d1081349d8133804004f8fe0))
* **Storage:** prevent loading reset on cancelled fetch ([625159a](https://github.com/ydb-platform/ydb-embedded-ui/commit/625159a396e1ab84fe9da94d047da67fdd03b30f))
* **Storage:** shrink tooltip active area on FQDN ([7c33d5a](https://github.com/ydb-platform/ydb-embedded-ui/commit/7c33d5afb561efa64f90ce5b93edd30f7d27c247))
* **Tenant:** prevent selected tab reset on tree navigation ([a4e633a](https://github.com/ydb-platform/ydb-embedded-ui/commit/a4e633aa45c803503fe69e52f0f2cfac4c6aae0d))
* **Tenant:** show loader when fetching overview data ([ae77495](https://github.com/ydb-platform/ydb-embedded-ui/commit/ae77495faa687652040a1f2965700184220778b4))
* use correct prop for textinputs value ([de97ba1](https://github.com/ydb-platform/ydb-embedded-ui/commit/de97ba17ba8da54a626509cf08f147f9fcc67004))
* **useAutofetcher:** pass argument to indicate background fetch ([4063cb1](https://github.com/ydb-platform/ydb-embedded-ui/commit/4063cb1411338d351b612fc46c06bcc708fe32f1))

## [2.2.1](https://github.com/ydb-platform/ydb-embedded-ui/compare/v2.2.0...v2.2.1) (2022-10-19)


### Bug Fixes

* revert prettier config, fix build ([c47dddf](https://github.com/ydb-platform/ydb-embedded-ui/commit/c47dddf834eadfd5642af62e0cc94f7567ec68fd))

## [2.2.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v2.1.0...v2.2.0) (2022-10-14)


### Features

* **Healthcheck:** rework issues list in modal ([e7cb0df](https://github.com/ydb-platform/ydb-embedded-ui/commit/e7cb0df58e22c8c9cd25aae83b78be4808e9ba81))


### Bug Fixes

* **EntityStatus:** enable component to left trim links ([fbc6c51](https://github.com/ydb-platform/ydb-embedded-ui/commit/fbc6c51f9fbea3c1a7f5f70cb542971a41f4d8b3))
* fix pre-commit prettier linting and add json linting ([#189](https://github.com/ydb-platform/ydb-embedded-ui/issues/189)) ([047415d](https://github.com/ydb-platform/ydb-embedded-ui/commit/047415d2d69ecf4a2d99f0092b9e6735bd8efbc0))
* **Healthcheck:** delete unneeded i18n translations ([0c6de90](https://github.com/ydb-platform/ydb-embedded-ui/commit/0c6de9031607e4cde1387387393a9cfc9e1e2b8f))
* **Healthcheck:** enable update button in modal to fetch data ([de0b06e](https://github.com/ydb-platform/ydb-embedded-ui/commit/de0b06e7f2d3536df1b3896cbf86a947b2e7a291))
* **Healthcheck:** fix layout shift on scrollbar appearance ([ccdde6e](https://github.com/ydb-platform/ydb-embedded-ui/commit/ccdde6e065abbdb1c22a2c3bdd17e63f706d0f77))
* **Healthcheck:** fix styles for long issues trees ([32f1a8d](https://github.com/ydb-platform/ydb-embedded-ui/commit/32f1a8db58d9f84073327b92dcd80a5b4626a526))
* **Healthcheck:** fix variable typo ([0f0e056](https://github.com/ydb-platform/ydb-embedded-ui/commit/0f0e056576b9ec18fc3ce574d3742d55e5da6e35))
* **Healthcheck:** full check status in a preview ([bc0b51e](https://github.com/ydb-platform/ydb-embedded-ui/commit/bc0b51eedd4ff3b4ae1650946832f463a6703c12))
* **Healthcheck:** make modal show only one first level issue ([cdc95a7](https://github.com/ydb-platform/ydb-embedded-ui/commit/cdc95a7412c1266d990df7e2807630a8f4c88780))
* **Healthcheck:** redesign healthcheck header ([867f57a](https://github.com/ydb-platform/ydb-embedded-ui/commit/867f57aed84b7b72c22a816c6ac02387490ff495))
* **Healthcheck:** replace update button with icon ([709a994](https://github.com/ydb-platform/ydb-embedded-ui/commit/709a994544f068db1b0fe09009ecb4d8db46fc38))
* **Healthcheck:** update styles to be closer to the design ([aa1083d](https://github.com/ydb-platform/ydb-embedded-ui/commit/aa1083d299e24590336eeb3d913a9c53fd77bad6))
* **Nodes:** case insensitive search ([11d2c98](https://github.com/ydb-platform/ydb-embedded-ui/commit/11d2c985e0c30bb74ed07e22273d8b3459b54c89))
* **QueryEditor:** smarter error message trim ([8632948](https://github.com/ydb-platform/ydb-embedded-ui/commit/863294828090dc8eb2595884283d0996156c3785))
* **Tenants:** case insensitive search ([0ad93f5](https://github.com/ydb-platform/ydb-embedded-ui/commit/0ad93f57dcbba7d9746be54a4ba7b76ab4d45108))
* **Tenants:** fix filtering by ControlPlane name ([4941c82](https://github.com/ydb-platform/ydb-embedded-ui/commit/4941c821cdbb7c5d0da26a3b0d5c00d8979401c0))
* **Tenants:** left trim db names in db list ([81bf0fa](https://github.com/ydb-platform/ydb-embedded-ui/commit/81bf0fafe901d3601dc04fdf71939e914493ff1c))

## [2.1.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v2.0.0...v2.1.0) (2022-10-04)


### Features

* autofocus all text search fields ([a38ee84](https://github.com/ydb-platform/ydb-embedded-ui/commit/a38ee84abad4202f5e9b8af897eb68d2c006233a))
* **Healthcheck:** display first level issues in overview ([10b4bf5](https://github.com/ydb-platform/ydb-embedded-ui/commit/10b4bf5d15d32f028702ff8cfecca0e06bc5616f))


### Bug Fixes

* fix production assets paths ([8eaad0f](https://github.com/ydb-platform/ydb-embedded-ui/commit/8eaad0f1db109c4cf3cbf7d11ad32ea335a6b0c1))
* **Healthcheck:** add translations ([75f9851](https://github.com/ydb-platform/ydb-embedded-ui/commit/75f9851a35766ef692805a6f154d40340b003487))
* move eslint hooks rule extension to src config ([179b81d](https://github.com/ydb-platform/ydb-embedded-ui/commit/179b81d60adf422addc8d72f947800c72bd3e4c5))
* **QueryEditor:** disable fullscreen button for empty result ([4825b5b](https://github.com/ydb-platform/ydb-embedded-ui/commit/4825b5b8dcb89fcafd828dabbace521ddc429922))
* **QueryEditor:** fix query stats spacings ([b836d72](https://github.com/ydb-platform/ydb-embedded-ui/commit/b836d72824a791b3fde2b9e4585c6c9b42385265))
* **useAutofetcher:** private autofetcher instance for each usage ([3f34b7a](https://github.com/ydb-platform/ydb-embedded-ui/commit/3f34b7aee2042562a42e6d1a7daf03ffddd888c0))

## [2.0.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v1.14.2...v2.0.0) (2022-09-26)


### ⚠ BREAKING CHANGES

* peer deps update: migrated from `@yandex-cloud/uikit` to `@gravity-ui/uikit`


### Bug Fixes

* **QueryEditor:** adjust execute issues scrollbar position ([8b03400](https://github.com/ydb-platform/ydb-embedded-ui/commit/8b03400aa084a660f44dced437a97e4b956704d6))
* **QueryEditor:** adjust explain components position ([193d326](https://github.com/ydb-platform/ydb-embedded-ui/commit/193d3263c2c9b57381f8d5ba160b95e76b5d32af))
* **QueryEditor:** properly handle empty query explanations ([5943d1b](https://github.com/ydb-platform/ydb-embedded-ui/commit/5943d1b38534e26729310e34aa24dc30a658a0fa))
* **QueryEditor:** render v2 explain with default topology ([44947e1](https://github.com/ydb-platform/ydb-embedded-ui/commit/44947e10248f5d14d0d685a030e2dbca0c87399d))
* **QueryEditor:** use modern explain query schema ([78acc45](https://github.com/ydb-platform/ydb-embedded-ui/commit/78acc45765d9f9ff45a37934be61559373b5c07c))
* **Storage:** encouraging message for empty filtered lists ([028aa8d](https://github.com/ydb-platform/ydb-embedded-ui/commit/028aa8db2ddff9f64d1b6ac6543d7d640a3187a9))
* **Tenant:** adjust info tab spacings ([89e5809](https://github.com/ydb-platform/ydb-embedded-ui/commit/89e580939766c2ed4018b4e46c3b34d8744a9957))
* **Tenant:** display 0 values in columns tables info ([ba2cbde](https://github.com/ydb-platform/ydb-embedded-ui/commit/ba2cbde662471dfbe34892154aa2211088100f31))
* **Tenant:** modern query response for column tables ([ab2e45f](https://github.com/ydb-platform/ydb-embedded-ui/commit/ab2e45f4df33a2366f3a673b1beab97f3d76a3a4))
* **Tenant:** properly fetch column tables data for info tab ([8762746](https://github.com/ydb-platform/ydb-embedded-ui/commit/8762746d9c89faeea25f9f47107b6d93fffee918))
* **TopQueries:** modern query response ([fe2b45a](https://github.com/ydb-platform/ydb-embedded-ui/commit/fe2b45a15b25c4d1ca8324e9727bee9194bdb9bc))
* **TopShards:** modern query response ([3f847eb](https://github.com/ydb-platform/ydb-embedded-ui/commit/3f847eb23fe1fca216e2026764a897cbafd56a38))
* **UserSettings:** save invertedDisks as string ([d41dcc6](https://github.com/ydb-platform/ydb-embedded-ui/commit/d41dcc68d4eff47ddb54781e1bbd8192ba669500))

## [1.14.2](https://github.com/ydb-platform/ydb-embedded-ui/compare/v1.14.1...v1.14.2) (2022-09-19)


### Bug Fixes

* process new explain format ([2ede9ab](https://github.com/ydb-platform/ydb-embedded-ui/commit/2ede9ab11a29667204cca110858b0cca74588255))
* process new query format ([eb880be](https://github.com/ydb-platform/ydb-embedded-ui/commit/eb880be36b99efe7f0c0ff96b58401293ff080e1))

## [1.14.1](https://github.com/ydb-platform/ydb-embedded-ui/compare/v1.14.0...v1.14.1) (2022-09-16)


### Bug Fixes

* **Tenants:** display nodes count 0 for undefined NodeIds ([4be42ec](https://github.com/ydb-platform/ydb-embedded-ui/commit/4be42eca84557929837e799d7d8dcebd858470d4))

## [1.14.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v1.13.2...v1.14.0) (2022-09-16)


### Features

* **Preview:** use modern query schema ([60bed3f](https://github.com/ydb-platform/ydb-embedded-ui/commit/60bed3fcb0fd76b869883742a2f2911201c0c226))
* **QueryEditor:** use modern query schema ([ecf38aa](https://github.com/ydb-platform/ydb-embedded-ui/commit/ecf38aa6b164ef7705e004aa77c8dab0e3164b51))
* **QueryResultTable:** component for displaying query result ([1b8be10](https://github.com/ydb-platform/ydb-embedded-ui/commit/1b8be10546ad9ae13b1043b2871b2aa110a5b6d4))
* **Storage:** experimental settings for disk colors ([b4291f4](https://github.com/ydb-platform/ydb-embedded-ui/commit/b4291f4ca19c588bc17eca50da51e898e6ccf581))
* **Tenant:** cdc streams info ([4cc773f](https://github.com/ydb-platform/ydb-embedded-ui/commit/4cc773f0351e3f1f6e279d1bebbb78329695e9ae))
* **Tenant:** cdc streams overview ([d1aed44](https://github.com/ydb-platform/ydb-embedded-ui/commit/d1aed4467135adaf01a06f8c4c4a4b3eb0b53106))
* **Tenant:** pq groups info & overview ([e1878a6](https://github.com/ydb-platform/ydb-embedded-ui/commit/e1878a6353933f74e62b204bf210f56a18a16c49))
* **Tenants:** display tenant nodes count ([72aef25](https://github.com/ydb-platform/ydb-embedded-ui/commit/72aef250006aae53d7704ff539b9eb537e6bfbd4))
* use schema param in sendQuery api ([01f9c71](https://github.com/ydb-platform/ydb-embedded-ui/commit/01f9c71190622279f03cd1c01d6b6e8e6739362a))


### UI Updates

* **Storage:** new disks design ([26033d2](https://github.com/ydb-platform/ydb-embedded-ui/commit/26033d21e994c6ece7b3b8999d0fabbf82b43021))
* **Tenant:** consistent paddings for query results ([7f0a7c2](https://github.com/ydb-platform/ydb-embedded-ui/commit/7f0a7c28d18e48013223239b5780dbaca18f68a8))


### Bug Fixes

* always parse query error to string ([0fcabf7](https://github.com/ydb-platform/ydb-embedded-ui/commit/0fcabf7042adfc728f1ec651ebae50e8c40e9199))
* correct types & parsing for query api response ([d6a177c](https://github.com/ydb-platform/ydb-embedded-ui/commit/d6a177cd0e726f1d19e27c642e0a9c1d2832bbe0))
* **Preview:** display "table is empty" only for tables ([21a93c1](https://github.com/ydb-platform/ydb-embedded-ui/commit/21a93c1a070dbd04f7338537200cd2cb9849ff88))
* **Preview:** fix action type id ([7793fad](https://github.com/ydb-platform/ydb-embedded-ui/commit/7793fad6b618bfc4c35b85481b2a0b794698eaa1))
* **QueryResultTable:** don't display absent result as empty ([e2e5bfa](https://github.com/ydb-platform/ydb-embedded-ui/commit/e2e5bfaf0dbb89ec64766bf4ed5a4fab10ae8844))
* **QueryResultTable:** don't require theme prop ([c9686d4](https://github.com/ydb-platform/ydb-embedded-ui/commit/c9686d46eb2efdeb4bc093ecd44619e6c1a9c2fd))
* **Tenant:** input working query for 'select query' action in schema ([de152bd](https://github.com/ydb-platform/ydb-embedded-ui/commit/de152bdcc38fd6f4b1e5a5e6102c621f0155be36))
* **Tenant:** rename tab overview -> info ([2d13ffe](https://github.com/ydb-platform/ydb-embedded-ui/commit/2d13ffeb149765680c2887ea7ffb86d68ac92d5c))

## [1.13.2](https://github.com/ydb-platform/ydb-embedded-ui/compare/v1.13.1...v1.13.2) (2022-09-05)


### Bug Fixes

* **Tenant:** fix acl scroll ([161bc8d](https://github.com/ydb-platform/ydb-embedded-ui/commit/161bc8d507de126c1383a10713e2ffaaaf13301d))
* **Tenant:** fix layout after moving tabs ([6abfded](https://github.com/ydb-platform/ydb-embedded-ui/commit/6abfdedb97345b555be306d49ea2454f35de9bb4))
* **Tenant:** load root if cahced path is not in tree ([2d86044](https://github.com/ydb-platform/ydb-embedded-ui/commit/2d8604464711a638dbd20cf8a14142b0de3e3a95))

## [1.13.1](https://github.com/ydb-platform/ydb-embedded-ui/compare/v1.13.0...v1.13.1) (2022-09-02)


### Bug Fixes

* **Storage:** fix groups/nodes counter ([9b59ae0](https://github.com/ydb-platform/ydb-embedded-ui/commit/9b59ae0d045beff7aa45560e028618a88bd8483f))

## [1.13.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v1.12.2...v1.13.0) (2022-09-01)


### Features

* **Storage:** add usage filter component ([a35067f](https://github.com/ydb-platform/ydb-embedded-ui/commit/a35067f8c34ad5d3faf4fb9381c0d6023df9afbd))
* **Storage:** usage filter ([276f027](https://github.com/ydb-platform/ydb-embedded-ui/commit/276f0270a458601929624a4872ec81e001931853))


### Bug Fixes

* **Storage:** properly debounce text input filter ([bc5e8fd](https://github.com/ydb-platform/ydb-embedded-ui/commit/bc5e8fd7b067b850f0376b55d995213292b8a31e))
* **Storage:** use current list size for counter ([e6fea58](https://github.com/ydb-platform/ydb-embedded-ui/commit/e6fea58b075de4c35ad8a60d339417c1e7204d83))
* **Tenant:** move general tabs outside navigation ([5bf21ea](https://github.com/ydb-platform/ydb-embedded-ui/commit/5bf21eac6f38c0392c8dc6e04be1b6fd0e147064))

## [1.12.2](https://github.com/ydb-platform/ydb-embedded-ui/compare/v1.12.1...v1.12.2) (2022-08-29)


### Bug Fixes

* **Storage:** bright red usage starting from 90% ([69b7ed2](https://github.com/ydb-platform/ydb-embedded-ui/commit/69b7ed248151f518ffc5fabbdccf5ea9bbcd9405))
* **Storage:** display usage without gte sign ([39630a2](https://github.com/ydb-platform/ydb-embedded-ui/commit/39630a2a06b574d53d0ef74c1b3e0dc96b9666a8))

## [1.12.1](https://github.com/ydb-platform/ydb-embedded-ui/compare/v1.12.0...v1.12.1) (2022-08-26)


### Bug Fixes

* **Storage:** properly display usage for 0 storage ([aee67f9](https://github.com/ydb-platform/ydb-embedded-ui/commit/aee67f9314341c995e2c9468f5eedc48fa0a3d35))

## [1.12.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v1.11.1...v1.12.0) (2022-08-26)


### Features

* **Storage:** show usage column ([73aed5f](https://github.com/ydb-platform/ydb-embedded-ui/commit/73aed5f9ed60b6d2bd77fd315ae514ee7443c489))
* **Storage:** vividly show degraded disks count ([7315a9c](https://github.com/ydb-platform/ydb-embedded-ui/commit/7315a9cfd98002a7fab85d721712aa82c6dbb552))

## [1.11.1](https://github.com/ydb-platform/ydb-embedded-ui/compare/v1.11.0...v1.11.1) (2022-08-26)


### Bug Fixes

* number type instead of string for uint32 ([e60799e](https://github.com/ydb-platform/ydb-embedded-ui/commit/e60799edec4ef831e8c0d51f4384cde83520541d))
* **Storage:** expect arbitrary donors data ([09f8e08](https://github.com/ydb-platform/ydb-embedded-ui/commit/09f8e085c94faacd9da502643355e932346502ac))
* vdisk data contains pdisk data, not id ([bd1ea7f](https://github.com/ydb-platform/ydb-embedded-ui/commit/bd1ea7f59e0461256bb12f146b50470d21ac1ace))

## [1.11.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v1.10.3...v1.11.0) (2022-08-23)


### Features

* **Stack:** new component for stacked elements ([c42ba37](https://github.com/ydb-platform/ydb-embedded-ui/commit/c42ba37fafdd9dedc4be9d625d7e756a83c01fe3))
* **Storage:** display donor disks ([b808fe9](https://github.com/ydb-platform/ydb-embedded-ui/commit/b808fe951987c615f797af56017f8045a1ed852f))
* **VDisk:** display label for donors ([bba5ae8](https://github.com/ydb-platform/ydb-embedded-ui/commit/bba5ae8e44347a5b1d9cb72424f5a963a6848e59))


### Bug Fixes

* **InfoViewer:** add size_s ([fc06451](https://github.com/ydb-platform/ydb-embedded-ui/commit/fc0645118f64a79f660d734c2ff43c42c738fd40))
* **PDisk:** new popup design ([9c0355d](https://github.com/ydb-platform/ydb-embedded-ui/commit/9c0355d4d9ccf69d43a5287b0e78d7c7993c4a18))
* **PDisk:** restrict component interface ([328efa9](https://github.com/ydb-platform/ydb-embedded-ui/commit/328efa90d214eca1bceeeb5bd9099aab36a3ddb0))
* **Storage:** shrink tooltip active area on Pool Name ([30a2b92](https://github.com/ydb-platform/ydb-embedded-ui/commit/30a2b92ff598d9caeabe17a4b8de214943945a91))
* **VDisk:** add a missing prop type ([39b6cf3](https://github.com/ydb-platform/ydb-embedded-ui/commit/39b6cf38811cab6c4374c77d3eb63c11fa7b83d5))
* **VDisk:** don't paint donors blue ([6b148b9](https://github.com/ydb-platform/ydb-embedded-ui/commit/6b148b914663a74e528a01a35f575f87ed6e9f09))
* **VDisk:** new popup design ([107b139](https://github.com/ydb-platform/ydb-embedded-ui/commit/107b13900b08631ea42034a6a2f7961c49c86556))

## [1.10.3](https://github.com/ydb-platform/ydb-embedded-ui/compare/v1.10.2...v1.10.3) (2022-08-23)


### Bug Fixes

* **Overview:** format undefined values to empty string, not 0 ([1a37c27](https://github.com/ydb-platform/ydb-embedded-ui/commit/1a37c278328ad8eb4397d9507566829f01a9c872))

## [1.10.2](https://github.com/ydb-platform/ydb-embedded-ui/compare/v1.10.1...v1.10.2) (2022-08-17)


### Bug Fixes

* convert bytes on decimal scale ([db9b0a7](https://github.com/ydb-platform/ydb-embedded-ui/commit/db9b0a71fc5334f5a40992cc6abc0688782ad5d2))
* display HDD instead of ROT as pdisk type ([bd9e5ba](https://github.com/ydb-platform/ydb-embedded-ui/commit/bd9e5ba4e594cb3a1f6a964f619f9824e083ae7c))
* **InfoViewer:** accept default value formatter ([e03d8cc](https://github.com/ydb-platform/ydb-embedded-ui/commit/e03d8cc5de76e4ac00b05586ae6f6522a9708fb0))
* **InfoViewer:** allow longer labels ([89060a3](https://github.com/ydb-platform/ydb-embedded-ui/commit/89060a381858b5beaa3c3cf3402c13c917705676))
* **Overview:** display table r/o replicas ([6dbe0b4](https://github.com/ydb-platform/ydb-embedded-ui/commit/6dbe0b45fc5e3867f9d6141d270c15508a693e35))
* **Overview:** format & group table info in overview ([1a35cfc](https://github.com/ydb-platform/ydb-embedded-ui/commit/1a35cfcd2075454c4a1f1fc4961a4b3106b6d225))
* **QueryEditor:** save chosen run action ([b0fb436](https://github.com/ydb-platform/ydb-embedded-ui/commit/b0fb43651e0c6d1dc5d6a25f92716703402b556d))
* use current i18n lang for numeral formatting ([5d58fcf](https://github.com/ydb-platform/ydb-embedded-ui/commit/5d58fcffde21924f3cbe6c28946c7a9f755a8490))

## [1.10.1](https://github.com/ydb-platform/ydb-embedded-ui/compare/v1.10.0...v1.10.1) (2022-08-10)


### Bug Fixes

* **Tenant:** fix actions set for topics ([0c75bf4](https://github.com/ydb-platform/ydb-embedded-ui/commit/0c75bf4561966dd663ab1cd7c7b81ef6b4632e50))

## [1.10.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v1.9.0...v1.10.0) (2022-08-10)


### Features

* **TopShards:** add DataSize column ([cbcd047](https://github.com/ydb-platform/ydb-embedded-ui/commit/cbcd047d277f699a67bc002a5542f3b9f6a0c942))
* **TopShards:** sort table data on backend ([dc28c5c](https://github.com/ydb-platform/ydb-embedded-ui/commit/dc28c5c75b0036480bf804d49f82fc54eac98c8e))


### Bug Fixes

* add concurrentId for sendQuery request ([dc6b32a](https://github.com/ydb-platform/ydb-embedded-ui/commit/dc6b32a8fd51064ddeca2fc60a0f08a725216334))
* **Storage:** display pdisk type in tooltip ([2b03a35](https://github.com/ydb-platform/ydb-embedded-ui/commit/2b03a35fc11ddeae3bdd30a0690b324ae917f5c3))
* **Tablet:** change Kill to Restart ([dd585b1](https://github.com/ydb-platform/ydb-embedded-ui/commit/dd585b1d1a6a5ddb484a702523773b169900f582))
* **Tenant:** add missing schema node types ([62a0ecb](https://github.com/ydb-platform/ydb-embedded-ui/commit/62a0ecb848dbcee53e18535cbf7c03a731d0cfeb))
* **Tenant:** ensure correct behavior for new schema node types ([f80c381](https://github.com/ydb-platform/ydb-embedded-ui/commit/f80c38152656e8bbbe51ec38b29fc0d954c361cc))
* **Tenant:** use new schema icons ([389a921](https://github.com/ydb-platform/ydb-embedded-ui/commit/389a9214c64b1adb183fa0c6caa6f2ec536dbef3))
* **TopShards:** disable virtualization for table ([006d3d9](https://github.com/ydb-platform/ydb-embedded-ui/commit/006d3d9fb9a4744b8bb4ad03e53693199213f80e))
* **TopShards:** format DataSize value ([c51ce66](https://github.com/ydb-platform/ydb-embedded-ui/commit/c51ce66286f6454f7252d1194628ee5a50aafba2))
* **TopShards:** only allow DESC sort ([6aa326f](https://github.com/ydb-platform/ydb-embedded-ui/commit/6aa326fc4b8165f00f8b3ecf5becdb0943ed57af))
* **TopShards:** substring tenant name out of shards path ([9e57672](https://github.com/ydb-platform/ydb-embedded-ui/commit/9e5767222c7dac7734c68abd08067cea507b1e15))

## [1.9.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v1.8.8...v1.9.0) (2022-07-29)


### Features

* **Node:** display endpoints in overview ([89e9e47](https://github.com/ydb-platform/ydb-embedded-ui/commit/89e9e470499b6f458e8949211d97293c0b7d9b97))
* **Node:** display node basic info above tabs ([aafb15b](https://github.com/ydb-platform/ydb-embedded-ui/commit/aafb15b399bf116026eff36f3c4ac817e2c40e18))
* **Node:** more informative pdisks panels ([342712b](https://github.com/ydb-platform/ydb-embedded-ui/commit/342712bcaa793971e1ca354da57fb962639ef90c))
* **Nodes:** show node endpoints in tooltip ([34be559](https://github.com/ydb-platform/ydb-embedded-ui/commit/34be55957e02f947ede30b43f22fde82d21df308))
* **Tenant:** table index overview ([2aed714](https://github.com/ydb-platform/ydb-embedded-ui/commit/2aed71488cde1175e6569c236ab609bb126f9cf3))
* **Tenant:** virtualized tree in schema ([815f558](https://github.com/ydb-platform/ydb-embedded-ui/commit/815f5588e5fed6fb86f69653c4937e975465372f))
* utils for parsing bitfields in pdisk data ([da22b4a](https://github.com/ydb-platform/ydb-embedded-ui/commit/da22b4afde9efe4d9605cefb69ddd51aed989722))


### Bug Fixes

* **Node:** fix pdisk title items width ([ca5fec6](https://github.com/ydb-platform/ydb-embedded-ui/commit/ca5fec6388364b7d1d6362f1bda36431d9c29749))
* **Nodes:** hide tooltip on unmount ([54e4fdc](https://github.com/ydb-platform/ydb-embedded-ui/commit/54e4fdc8045c555338e79d89a93faf58e888fa0e))
* **ProgressViewer:** apply provided custom class name ([aa60e9d](https://github.com/ydb-platform/ydb-embedded-ui/commit/aa60e9d1b9c0752853f4323d3bcfd220bedd272d))
* **Tenant:** display all table props in overview ([d70e311](https://github.com/ydb-platform/ydb-embedded-ui/commit/d70e311296f6a4d1781f6e72929c70e0db7c3226))
* **Tenant:** display PartCount first in table overview ([8c09746](https://github.com/ydb-platform/ydb-embedded-ui/commit/8c09746b026a23a36fe31be94057cc92535aceaa))

## [1.8.8](https://github.com/ydb-platform/ydb-embedded-ui/compare/v1.8.7...v1.8.8) (2022-07-21)


### Bug Fixes

* **TabletsFilters:** display tablets grid full-height ([0dde809](https://github.com/ydb-platform/ydb-embedded-ui/commit/0dde8097fe026248aade97f034fa35c56b28e903))
* **TabletsOverall:** properly hide tooltip on mouseleave ([df36eba](https://github.com/ydb-platform/ydb-embedded-ui/commit/df36ebaf44d8966bc419f3720d51390dfd767a87))
* **Tablets:** properly display tablets in grid ([f3b64fa](https://github.com/ydb-platform/ydb-embedded-ui/commit/f3b64fae3a1e1a46ababd2d2f04ddff488698676))
* **Tenant:** align info in overview ([acb39fa](https://github.com/ydb-platform/ydb-embedded-ui/commit/acb39fab70b7b4e0e78124fd887b2f1b76815221))
* **Tenant:** display tenant name in single line ([301e391](https://github.com/ydb-platform/ydb-embedded-ui/commit/301e3911330024f80ebfda6d1a16823b64d94b36))
* **Tenant:** move tablets under tenant name ([b7e4b8f](https://github.com/ydb-platform/ydb-embedded-ui/commit/b7e4b8f7027f1481a7c1baff77bf8ad5e2ed467c))

## [1.8.7](https://github.com/ydb-platform/ydb-embedded-ui/compare/v1.8.6...v1.8.7) (2022-07-18)


### Bug Fixes

* **Preview:** sort numbers as numbers, not string ([6c42a62](https://github.com/ydb-platform/ydb-embedded-ui/commit/6c42a62d077fcb9419ceb680906d4cef78a0134f))

## [1.8.6](https://github.com/ydb-platform/ydb-embedded-ui/compare/v1.8.5...v1.8.6) (2022-07-14)


### Bug Fixes

* **Tenant:** fix switching between groups and nodes on storage tab ([6923885](https://github.com/ydb-platform/ydb-embedded-ui/commit/6923885336fa21ac985879e41685137adbf8159a))

## [1.8.5](https://github.com/ydb-platform/ydb-embedded-ui/compare/v1.8.4...v1.8.5) (2022-07-11)


### Bug Fixes

* **AsideNavigation:** aside header is compact by default ([aa3ad03](https://github.com/ydb-platform/ydb-embedded-ui/commit/aa3ad033fc6b62e6f2ee595e266343e67e764ec6))

## [1.8.4](https://github.com/ydb-platform/ydb-embedded-ui/compare/v1.8.3...v1.8.4) (2022-07-11)


### Bug Fixes

* **Nodes:** add /internal for nodes external link ([a649dd2](https://github.com/ydb-platform/ydb-embedded-ui/commit/a649dd209bae4abd6916f23d0894df893602aaf7))

## [1.8.3](https://github.com/ydb-platform/ydb-embedded-ui/compare/v1.8.2...v1.8.3) (2022-07-08)


### Bug Fixes

* timeout 600 sec for requests /viewer/json/query ([cf65122](https://github.com/ydb-platform/ydb-embedded-ui/commit/cf651221f866e5f56ecf6c900b3778dedc31eb95))

## [1.8.2](https://github.com/ydb-platform/ydb-embedded-ui/compare/v1.8.1...v1.8.2) (2022-07-07)


### Bug Fixes

* **Tenant:** 3 tabs for indexes ([9280384](https://github.com/ydb-platform/ydb-embedded-ui/commit/9280384733938c4bd269bf6f9adf23efb552c6e8))
* **Tenant:** hide preview button for index tables ([a25e0ea](https://github.com/ydb-platform/ydb-embedded-ui/commit/a25e0ea0413277e27c54d123e2be7a15b8a2aaa4))

## [1.8.1](https://github.com/ydb-platform/ydb-embedded-ui/compare/v1.8.0...v1.8.1) (2022-07-06)


### Bug Fixes

* **Tenant:** diagnostics view for table indexes ([63d3133](https://github.com/ydb-platform/ydb-embedded-ui/commit/63d3133c0d61f6d39186f0c5df2eb6983a9c8bf7))
* **Tenant:** own context actions for table indexes ([3cd946a](https://github.com/ydb-platform/ydb-embedded-ui/commit/3cd946a333be402cec70569affef5865b0dd8934))

## [1.8.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v1.7.1...v1.8.0) (2022-07-05)


### Features

* add Illustration component ([7d10880](https://github.com/ydb-platform/ydb-embedded-ui/commit/7d10880cd4d9f945e7c8a7232327d8db68f0865c))
* **Tenant:** proper 403 error page ([d822a2b](https://github.com/ydb-platform/ydb-embedded-ui/commit/d822a2b6e3e18c24882ecf30db399087053b83b3))


### Bug Fixes

* fix empty state illustration layout ([7cfd97e](https://github.com/ydb-platform/ydb-embedded-ui/commit/7cfd97e13ebcaa703478bd7b4e29774150bd569e))

## [1.7.1](https://github.com/ydb-platform/ydb-embedded-ui/compare/v1.7.0...v1.7.1) (2022-07-05)


### Performance Improvements

* **Tenant:** use api call viewer/json/acl instead of metainfo ([c3603c4](https://github.com/ydb-platform/ydb-embedded-ui/commit/c3603c4b364cef79cb4790c7e9e4378d5b66e0ed))

## [1.7.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v1.6.4...v1.7.0) (2022-06-29)


### Features

* **Storage:** show total groups count ([5e31cfe](https://github.com/ydb-platform/ydb-embedded-ui/commit/5e31cfee9edc50fa4bc0770c443b136291a3536e))
* **Storage:** show total nodes count ([b438f70](https://github.com/ydb-platform/ydb-embedded-ui/commit/b438f7075961e878a1412ca185743c4374dd9178))
* **Tenant:** display tables indexes ([693a100](https://github.com/ydb-platform/ydb-embedded-ui/commit/693a1001db6487b2d43aeca7d8168afcd06f5cbd))

## [1.6.4](https://github.com/ydb-platform/ydb-embedded-ui/compare/v1.6.3...v1.6.4) (2022-06-24)


### Bug Fixes

* **Tenant:** properly display ColumnTables ([14d1e07](https://github.com/ydb-platform/ydb-embedded-ui/commit/14d1e074bf615be50f4f466d25e605b418f22b47))

## [1.6.3](https://github.com/ydb-platform/ydb-embedded-ui/compare/v1.6.2...v1.6.3) (2022-06-22)


### Bug Fixes

* **ClipboardButton:** clickable area now matches visual area ([8c0b5ef](https://github.com/ydb-platform/ydb-embedded-ui/commit/8c0b5ef27d5d31b28a29455b3019de23bdbf8f68))

## [1.6.2](https://github.com/ydb-platform/ydb-embedded-ui/compare/v1.6.1...v1.6.2) (2022-06-07)


### Bug Fixes

* shouls always select result tab ([98d4bcb](https://github.com/ydb-platform/ydb-embedded-ui/commit/98d4bcbc94bc2b9db9fb9b9cd5aced9f079ecdae))

## [1.6.1](https://github.com/ydb-platform/ydb-embedded-ui/compare/v1.6.0...v1.6.1) (2022-06-07)


### Bug Fixes

* should show Pending instead of Pendin ([0b93f80](https://github.com/ydb-platform/ydb-embedded-ui/commit/0b93f8000dffca27cd26321eb86f41e4f458faa6))
* should show query error even if no issues ([708bac5](https://github.com/ydb-platform/ydb-embedded-ui/commit/708bac56c2e671ec23e23c5055d0c0a9d419cd86))

## [1.6.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v1.5.3...v1.6.0) (2022-06-06)


### Features

* query issues displaying ([3ba4c25](https://github.com/ydb-platform/ydb-embedded-ui/commit/3ba4c2591542ef902eba4f7c44550f3c59618575))


### Bug Fixes

* code-review ([742c58a](https://github.com/ydb-platform/ydb-embedded-ui/commit/742c58a9bc4fa0dd0b24aa0119b7352e2be6fc8e))
* **package.json:** typecheck script ([111b525](https://github.com/ydb-platform/ydb-embedded-ui/commit/111b525f51a050010bbc03a3d0990be00c18ccd8))

### [1.5.3](https://github.com/ydb-platform/ydb-embedded-ui/compare/v1.5.2...v1.5.3) (2022-05-26)


### Bug Fixes

* explicitly set lang for ydb-ui-components i18n ([5684524](https://github.com/ydb-platform/ydb-embedded-ui/commit/5684524267e2cbf19a44de75b0e0b2bf98b617fd))
* proper icon size in uikit/Select ([a665d6d](https://github.com/ydb-platform/ydb-embedded-ui/commit/a665d6d829dae61ccf25566dd7b8cd1e46a743bb))
* update code for @yandex-cloud/uikit@^2.0.0 ([49d67a1](https://github.com/ydb-platform/ydb-embedded-ui/commit/49d67a1bddcba6fa138b5ebaeb280f16366b3329))


### chore

* update @yandex-cloud/uikit to 2.4.0 ([d2eb2e5](https://github.com/ydb-platform/ydb-embedded-ui/commit/d2eb2e5db147604ae346aea295ae22759712eaa4))
* add @yandex-cloud/uikit to peer deps ([9c9f599](https://github.com/ydb-platform/ydb-embedded-ui/commit/9c9f5997dcca1be5868d013da311a28e495e7faa))
* update ydb-ui-components to v2.0.1 ([3d6a8d3](https://github.com/ydb-platform/ydb-embedded-ui/commit/3d6a8d30ab2ab47203eb956904e891ae106c0bc7))

### [1.5.2](https://github.com/ydb-platform/ydb-embedded-ui/compare/v1.5.1...v1.5.2) (2022-05-26)


### Bug Fixes

* **Tenant:** always update diagnostics tabs for root ([db03266](https://github.com/ydb-platform/ydb-embedded-ui/commit/db03266fd7dd6e4588c1db0d109bdfaa8f693e2d))
* **Tenant:** don't use HistoryAPI and redux-location-state together ([c1bc562](https://github.com/ydb-platform/ydb-embedded-ui/commit/c1bc5621e3ead44b1b84e592f8d7106bbc918e37))

### [1.5.1](https://github.com/ydb-platform/ydb-embedded-ui/compare/v1.5.0...v1.5.1) (2022-05-25)


### Bug Fixes

* **Authentication:** submit form with enter in the login field ([7b6132a](https://github.com/ydb-platform/ydb-embedded-ui/commit/7b6132a6b2556939648167f30b08c5688b56ab98))

## [1.5.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v1.4.2...v1.5.0) (2022-05-24)


### Features

* **Healthcheck:** use TreeView in issues viewer ([bcd81e5](https://github.com/ydb-platform/ydb-embedded-ui/commit/bcd81e56dc613cf3e9f31d77d930b79e070372e4))
* **Tenant:** use NavigationTree for schemas ([f2867e1](https://github.com/ydb-platform/ydb-embedded-ui/commit/f2867e18898028ca265df46fcc8bfa4f929173f0))


### Bug Fixes

* **Healthcheck:** don't display reasonsItems in issues viewer ([f0a545f](https://github.com/ydb-platform/ydb-embedded-ui/commit/f0a545f7c70d449c121d64f8d1820e53b880a0fc))
* **Tenant:** add ellipsis to menu items inserting queries ([09135a2](https://github.com/ydb-platform/ydb-embedded-ui/commit/09135a2777ec9183ddf71bd2a4de66c5ef422ac8))
* **Tenant:** change messages for path copy toasts ([09adfa5](https://github.com/ydb-platform/ydb-embedded-ui/commit/09adfa52735bf706deb1ee9bf37f4bfa459b3758))
* **Tenant:** switch to query tab for inserted query ([991f156](https://github.com/ydb-platform/ydb-embedded-ui/commit/991f156ff819c58ff79146a44b57fb400729f325))

### [1.4.2](https://github.com/ydb-platform/ydb-embedded-ui/compare/v1.4.1...v1.4.2) (2022-05-23)


### UI Updates

* **QueryEditor:** replace warning for query losing with note about how query are saved ([89820ca](https://github.com/ydb-platform/ydb-embedded-ui/commit/89820ca7e2d02f880eb81d484b8947d599798d5f))


### Bug Fixes

* **QueryEditor:** confirm query deletion with enter ([d3dadbd](https://github.com/ydb-platform/ydb-embedded-ui/commit/d3dadbd0244fead5f41bd98445669c4f5ce23c43))
* **QueryEditor:** field autofocus in query save dialog ([9225238](https://github.com/ydb-platform/ydb-embedded-ui/commit/92252384dc68c40191f7898fff9a2c1106b0b2f1))
* **QueryEditor:** save query with enter ([5f9c450](https://github.com/ydb-platform/ydb-embedded-ui/commit/5f9c450aedc90f0e162515294a74000c006f9be7))

### [1.4.1](https://github.com/ydb-platform/ydb-embedded-ui/compare/v1.4.0...v1.4.1) (2022-05-17)


### UI Updates

* **Tenant:** add tenant name wrapper ([8176d28](https://github.com/ydb-platform/ydb-embedded-ui/commit/8176d28a5769b2b95d667ed960ad34d7a0d9bb4c))


### Bug Fixes

* **NodesTable:** align external link icon ([a379796](https://github.com/ydb-platform/ydb-embedded-ui/commit/a379796c6b8087f25f95ce3db4be33f18da71e04))

## [1.4.0](https://github.com/ydb-platform/ydb-embedded-ui/compare/v1.3.0...v1.4.0) (2022-05-16)


### Features

* **Tenant:** save initial tab preference ([7195d0f](https://github.com/ydb-platform/ydb-embedded-ui/commit/7195d0f7f5754c461555211515f80ea96464ca15))


### UI Updtaes

* **NodesTable:** don't reserve space for icons next to node fqdn ([8fcf1b3](https://github.com/ydb-platform/ydb-embedded-ui/commit/8fcf1b3269dee7ada83d7c5abcf44ad004191851))


### Bug Fixes

* **Tenant:** mapDispatchToProps types ([7dcaf56](https://github.com/ydb-platform/ydb-embedded-ui/commit/7dcaf561ec0c361d52d789b2ea3b1aba75339d83))

## [1.3.0](https://www.github.com/ydb-platform/ydb-embedded-ui/compare/v1.2.6...v1.3.0) (2022-05-12)


### Features

* **Storage:** red progress bars for unavailable disks ([17cf94d](https://www.github.com/ydb-platform/ydb-embedded-ui/commit/17cf94defb23681bc62c768d3282eed00c7e974d))

### [1.2.6](https://www.github.com/ydb-platform/ydb-embedded-ui/compare/v1.2.5...v1.2.6) (2022-05-05)


### Bug Fixes

* code-review ([1068339](https://www.github.com/ydb-platform/ydb-embedded-ui/commit/1068339d128fb44b661837b7d777b5e5f725a611))
* **Diagnostics:** layout ([2b11c35](https://www.github.com/ydb-platform/ydb-embedded-ui/commit/2b11c35c14cd1fa17d36bbeb2a371fb2fef3fb70))

### [1.2.5](https://www.github.com/ydb-platform/ydb-embedded-ui/compare/v1.2.4...v1.2.5) (2022-05-05)


### Bug Fixes

* **Node:** right padding on the storage page ([3a09d80](https://www.github.com/ydb-platform/ydb-embedded-ui/commit/3a09d8030c2b9d8f34675f3a790e19bba5b864e4))
* **Tenant:** fix horizontal scrollbar on diagnostics storage page ([017f5f3](https://www.github.com/ydb-platform/ydb-embedded-ui/commit/017f5f3470875824b11575c506837ab461a4e840))
* **Tenant:** keep acl heading at top ([7859fc6](https://www.github.com/ydb-platform/ydb-embedded-ui/commit/7859fc6a2e47071daf18b48a23974f2393e31417))
* **Storage:** move filters out of scrollable container ([66baaec](https://www.github.com/ydb-platform/ydb-embedded-ui/commit/66baaec170449c89da2d9f8e2170875c13334e68))
* **NodesViewer:** match default control styles ([c007674](https://www.github.com/ydb-platform/ydb-embedded-ui/commit/c0076742e78fdb87aadae9b22e073b923e7ca57e))

### [1.2.4](https://www.github.com/ydb-platform/ydb-embedded-ui/compare/v1.2.3...v1.2.4) (2022-05-05)


### Bug Fixes

* **Storage:** make 2 argument in getStorageInfo optional ([e349f8b](https://www.github.com/ydb-platform/ydb-embedded-ui/commit/e349f8b958756258b2d3790fbc9018c63b86498e))

### [1.2.3](https://www.github.com/ydb-platform/ydb-embedded-ui/compare/v1.2.2...v1.2.3) (2022-05-05)


### Bug Fixes

* **node reducer:** should specify concurrentId in getNodeStructure ([103c843](https://www.github.com/ydb-platform/ydb-embedded-ui/commit/103c843524e21af421954444774d68bda540ceae))

### [1.2.2](https://www.github.com/ydb-platform/ydb-embedded-ui/compare/v1.2.1...v1.2.2) (2022-05-04)


### Bug Fixes

* code-review ([288fda3](https://www.github.com/ydb-platform/ydb-embedded-ui/commit/288fda3cd207908e9b5c0486c4d486c6f2e17dd4))
* reducer clusterInfo should not be used ([1cafcbf](https://www.github.com/ydb-platform/ydb-embedded-ui/commit/1cafcbfb15f668b100cf6628b540b7cd234f6024))

### [1.2.1](https://www.github.com/ydb-platform/ydb-embedded-ui/compare/v1.2.0...v1.2.1) (2022-04-27)


### Bug Fixes

* **Vdisk:** should not fail if no node id passed ([d66686d](https://www.github.com/ydb-platform/ydb-embedded-ui/commit/d66686d0cbd9f61c4e106f6775db2fca226c922f))

## [1.2.0](https://www.github.com/ydb-platform/ydb-embedded-ui/compare/v1.1.3...v1.2.0) (2022-04-26)


### Features

* **Storage:** smoother loading state for storage table ([f7f38c4](https://www.github.com/ydb-platform/ydb-embedded-ui/commit/f7f38c455dd9abc3f898048081e90af9b460f922))


### Bug Fixes

* prevent ghost autofetch ([153d829](https://www.github.com/ydb-platform/ydb-embedded-ui/commit/153d8291d315f1dab001a69981a12e30d3d2aea9))

### [1.1.3](https://www.github.com/ydb-platform/ydb-embedded-ui/compare/v1.1.2...v1.1.3) (2022-04-20)


### Bug Fixes

* should prepare internal link correctly ([3da36e2](https://www.github.com/ydb-platform/ydb-embedded-ui/commit/3da36e22f6adbce6a1b14ac1afb0fb4aa46bb75f))

### [1.1.2](https://www.github.com/ydb-platform/ydb-embedded-ui/compare/v1.1.1...v1.1.2) (2022-04-19)


### Bug Fixes

* **ObjectSummary:** should correctly parse table creation time ([c9887dd](https://www.github.com/ydb-platform/ydb-embedded-ui/commit/c9887dd162720667dcbe3b4834b3b0ba5a9f3f6e))

### [1.1.1](https://www.github.com/ydb-platform/ydb-embedded-ui/compare/v1.1.0...v1.1.1) (2022-04-19)


### Bug Fixes

* add typecheck + fix type errors ([e6d9086](https://www.github.com/ydb-platform/ydb-embedded-ui/commit/e6d9086c46702a611f848c992377d18826ca2e23))
* **Node:** scroll to selected vdisk should not apply to undefined container ([7236a43](https://www.github.com/ydb-platform/ydb-embedded-ui/commit/7236a43655b935777abb5b8df228ae011ceb6bed))

## [1.1.0](https://www.github.com/ydb-platform/ydb-embedded-ui/compare/v1.0.4...v1.1.0) (2022-04-15)


### Features

* local precommit check ([d5da9b3](https://www.github.com/ydb-platform/ydb-embedded-ui/commit/d5da9b3fb89eeeb5461e7e14fe33964a8ed9078d))
* new Node Structure view ([5cf5dd3](https://www.github.com/ydb-platform/ydb-embedded-ui/commit/5cf5dd39fa59625be4bb89f16796f16ecb9d9d78))


### Bug Fixes

* **Authentication:** should be able to send authentication data with empty password [YDB-1610] ([5d4d881](https://www.github.com/ydb-platform/ydb-embedded-ui/commit/5d4d8810bb2ddabb9db1316a99194f5a1bd986b6))
* **Cluster:** should show additional info ([cb21ce3](https://www.github.com/ydb-platform/ydb-embedded-ui/commit/cb21ce317c55d05c7a7c166bc09dc1fe14e41692))
* code-review ([a706903](https://www.github.com/ydb-platform/ydb-embedded-ui/commit/a706903e6a30ee62aff5829c37ba8c197335e106))
* different interface fixes ([0bd3a32](https://www.github.com/ydb-platform/ydb-embedded-ui/commit/0bd3a32bf1502cc6d0f7419aa9d00653afe5d7bf))
* improve usability ([20f1acc](https://www.github.com/ydb-platform/ydb-embedded-ui/commit/20f1acc255876968ea366a860d33a12eecc5e74f))
* **Nodes:** default path to node should be Overview ([ac4add6](https://www.github.com/ydb-platform/ydb-embedded-ui/commit/ac4add6c1403ac2b9614f252fabf23b9e97ef2c2))
* query run type select should be styled as action button [YDB-1567] ([d06cd6a](https://www.github.com/ydb-platform/ydb-embedded-ui/commit/d06cd6ac72ccb8c7eef205fddb1153e6383baeea))
* **QueryEditor:** should resolve row key by index [YDB-1604] ([4acd2a3](https://www.github.com/ydb-platform/ydb-embedded-ui/commit/4acd2a30d03f2e45368587839549f4e5981f93dd))
* refactoring ([0c5aca5](https://www.github.com/ydb-platform/ydb-embedded-ui/commit/0c5aca5b96bc5d5e9c3f121aa1ffe394f3fbd28f))
* **Storage:** wording fixed [YDB-1552] ([431f77f](https://www.github.com/ydb-platform/ydb-embedded-ui/commit/431f77f090073037404639c686246d2f115d98f4))
* styles ([2725055](https://www.github.com/ydb-platform/ydb-embedded-ui/commit/2725055b0f25e711c73e2888da41cfaf2657b110))

### [1.0.4](https://www.github.com/ydb-platform/ydb-embedded-ui/compare/v1.0.3...v1.0.4) (2022-03-24)


### Bug Fixes

* freeze deps ([349dee8](https://www.github.com/ydb-platform/ydb-embedded-ui/commit/349dee8cbc7376e316e3cb87f5eb46142975de6c))
* styles ([502bc0b](https://www.github.com/ydb-platform/ydb-embedded-ui/commit/502bc0bd319a141e2d3e90787eae41abcd24e76d))

### [1.0.3](https://www.github.com/ydb-platform/ydb-embedded-ui/compare/v1.0.2...v1.0.3) (2022-03-21)


### Bug Fixes

* query status should not be shown when query is loading ([d214eee](https://www.github.com/ydb-platform/ydb-embedded-ui/commit/d214eee575b63341082f0be33163e3fce520df88))
* should set correct initial current index in queries history ([c3228d7](https://www.github.com/ydb-platform/ydb-embedded-ui/commit/c3228d7a6a0c810982db1bdbec7762889ac44ffa))
* **Storage:** wording fixed [YDB-1552] ([3f487ff](https://www.github.com/ydb-platform/ydb-embedded-ui/commit/3f487ff01117963760b676d14281e93e5f3002c0))

### [1.0.2](https://www.github.com/ydb-platform/ydb-embedded-ui/compare/v1.0.1...v1.0.2) (2022-03-11)


### Bug Fixes

* **Header:** add link to internal viewer ([64af24f](https://www.github.com/ydb-platform/ydb-embedded-ui/commit/64af24f8d78cf0d34466ac129be10c0764cce3d4))

### [1.0.1](https://www.github.com/ydb-platform/ydb-embedded-ui/compare/v1.0.0...v1.0.1) (2022-03-05)


### Bug Fixes

* **QueriesHistory:** should save history to local storage ([#8](https://www.github.com/ydb-platform/ydb-embedded-ui/issues/8)) ([57031ab](https://www.github.com/ydb-platform/ydb-embedded-ui/commit/57031ab16900e9d1112bbf506d5c777f94f883bb))

## [1.0.0](https://www.github.com/ydb-platform/ydb-embedded-ui/compare/v0.2.0...v1.0.0) (2022-03-01)


### Bug Fixes

* **ObjectSummary:** start time should be taken from current schema object ([e7511be](https://www.github.com/ydb-platform/ydb-embedded-ui/commit/e7511be61e5c8d2052ad3a2247a713f55049d3e6))
* **QueryEditor:** key bindings should work properly ([ebe59b3](https://www.github.com/ydb-platform/ydb-embedded-ui/commit/ebe59b3c838889ee81e308232e4c8d2ba23a1a3a))
* **QueryExplain:** should render graph in fullscreen view properly ([da511da](https://www.github.com/ydb-platform/ydb-embedded-ui/commit/da511da2fc1a36282ad99f20d5d6fd0b5b4ea05b))
* **README:** ui path should be correct ([e2668ef](https://www.github.com/ydb-platform/ydb-embedded-ui/commit/e2668ef329de4cf31fd31061dfe3b4ac091e0121))

## [0.2.0](https://www.github.com/ydb-platform/ydb-embedded-ui/compare/v0.1.0...v0.2.0) (2022-02-24)


### Features

* new design, refactoring ([#3](https://www.github.com/ydb-platform/ydb-embedded-ui/issues/3)) ([76d7cb0](https://www.github.com/ydb-platform/ydb-embedded-ui/commit/76d7cb0ebad5658a6654254a0376b1ecf203e696))

## 0.1.0 (2022-02-17)


### Features

* initial import ([9bf5e83](https://www.github.com/ydb-platform/ydb-embedded-ui/commit/9bf5e833e3d2d10897215f7d439b284a4c3c10df))
