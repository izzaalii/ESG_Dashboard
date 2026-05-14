"""
ESG Pipeline — Scope Resolver
==============================
Translates any scope descriptor (single device, tag/group, multi-group,
or full site) into a flat list of device IDs before computation begins.

The ScopeResolver is the only component that talks to the device registry.
Everything downstream receives plain lists of device IDs — no scope logic
leaks into the computation or aggregation layers.

Usage
-----
    from scope_resolver import Scope, ScopeResolver, InMemoryDeviceRegistry

    registry = InMemoryDeviceRegistry(
        tag_map={
            "HVAC":     ["DEV-001", "DEV-002", "DEV-003"],
            "Floor-A":  ["DEV-001", "DEV-004"],
            "Organic":  ["DEV-005", "DEV-006"],
        },
        site_map={
            "SITE-01": ["DEV-001", "DEV-002", "DEV-003", "DEV-004", "DEV-005", "DEV-006"],
        },
    )

    resolver = ScopeResolver(registry)

    # Single device
    ids = resolver.resolve(Scope.device("DEV-001"))          # ["DEV-001"]

    # One tag
    ids = resolver.resolve(Scope.group("HVAC"))               # ["DEV-001","DEV-002","DEV-003"]

    # Union of several tags — duplicates removed, order preserved
    ids = resolver.resolve(Scope.groups(["HVAC", "Floor-A"])) # ["DEV-001","DEV-002","DEV-003","DEV-004"]

    # All devices at a site
    ids = resolver.resolve(Scope.site("SITE-01"))             # all 6
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable


# ---------------------------------------------------------------------------
# Scope descriptor
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Scope:
    """
    Immutable descriptor of 'what to compute'.

    Exactly one of the four fields should be set; the rest default to None.
    Use the class-method factories rather than constructing directly.
    """
    device_id: str | None = None        # single device
    tag:       str | None = None        # one logical group / tag
    tags:      tuple[str, ...] | None = None   # union of several groups
    site_id:   str | None = None        # all devices at a physical site

    # ── Factories ──────────────────────────────────────────────────────────

    @classmethod
    def device(cls, did: str) -> "Scope":
        """Scope for a single device."""
        if not did:
            raise ValueError("device_id must be a non-empty string")
        return cls(device_id=did)

    @classmethod
    def group(cls, tag: str) -> "Scope":
        """Scope for all devices carrying a specific tag."""
        if not tag:
            raise ValueError("tag must be a non-empty string")
        return cls(tag=tag)

    @classmethod
    def groups(cls, tags: list[str]) -> "Scope":
        """Scope for the union of devices across several tags."""
        if not tags:
            raise ValueError("tags list must not be empty")
        return cls(tags=tuple(tags))

    @classmethod
    def site(cls, sid: str) -> "Scope":
        """Scope for all devices at a physical site."""
        if not sid:
            raise ValueError("site_id must be a non-empty string")
        return cls(site_id=sid)

    # ── Introspection ──────────────────────────────────────────────────────

    @property
    def scope_type(self) -> str:
        """Human-readable scope type string, matches GroupPipelinePayload meta field."""
        if self.device_id:
            return "device"
        if self.tag:
            return "group"
        if self.tags:
            return "multi_group"
        if self.site_id:
            return "site"
        raise ValueError("Empty scope — no field is set")

    @property
    def label(self) -> str:
        """Short human-readable label for display / logging."""
        if self.device_id:
            return self.device_id
        if self.tag:
            return self.tag
        if self.tags:
            return ", ".join(self.tags)
        if self.site_id:
            return self.site_id
        return "unknown"

    def __post_init__(self) -> None:
        filled = sum(
            v is not None
            for v in (self.device_id, self.tag, self.tags, self.site_id)
        )
        if filled == 0:
            raise ValueError("Scope must have at least one field set")
        if filled > 1:
            raise ValueError(
                "Scope must have exactly one field set; "
                "use Scope.groups() for multi-tag unions"
            )


# ---------------------------------------------------------------------------
# DeviceRegistry protocol
# ---------------------------------------------------------------------------

@runtime_checkable
class DeviceRegistry(Protocol):
    """
    Interface for the device tag/site database.

    Implement this against your actual storage backend (PostgreSQL, DynamoDB,
    a config YAML — anything).  The two concrete implementations below
    (InMemoryDeviceRegistry, ChainedDeviceRegistry) cover testing and
    composition use-cases.
    """

    def devices_for_tag(self, tag: str) -> list[str]:
        """Return all device IDs carrying the given tag. Empty list if unknown."""
        ...

    def devices_for_site(self, site_id: str) -> list[str]:
        """Return all device IDs at the given site. Empty list if unknown."""
        ...


# ---------------------------------------------------------------------------
# Concrete implementations
# ---------------------------------------------------------------------------

@dataclass
class InMemoryDeviceRegistry:
    """
    Simple registry backed by in-process dicts.

    Perfect for unit tests and local development.

    Parameters
    ----------
    tag_map  : {"tag_name": ["DEV-001", "DEV-002", ...], ...}
    site_map : {"site_id":  ["DEV-001", ...], ...}
    """
    tag_map:  dict[str, list[str]] = field(default_factory=dict)
    site_map: dict[str, list[str]] = field(default_factory=dict)

    def devices_for_tag(self, tag: str) -> list[str]:
        return list(self.tag_map.get(tag, []))

    def devices_for_site(self, site_id: str) -> list[str]:
        return list(self.site_map.get(site_id, []))

    # ── Mutation helpers (useful for test setup) ───────────────────────────

    def add_tag(self, tag: str, device_ids: list[str]) -> None:
        """Add or replace a tag's device list."""
        self.tag_map[tag] = list(device_ids)

    def add_site(self, site_id: str, device_ids: list[str]) -> None:
        """Add or replace a site's device list."""
        self.site_map[site_id] = list(device_ids)

    def tag_device(self, device_id: str, tag: str) -> None:
        """Append a single device to a tag, creating the tag if needed."""
        if tag not in self.tag_map:
            self.tag_map[tag] = []
        if device_id not in self.tag_map[tag]:
            self.tag_map[tag].append(device_id)

    def all_tags(self) -> list[str]:
        return list(self.tag_map.keys())

    def all_site_ids(self) -> list[str]:
        return list(self.site_map.keys())


@dataclass
class ChainedDeviceRegistry:
    """
    Fan-out registry that queries multiple registries in order and merges results.

    Useful when tags come from one DB and site membership from another.
    """
    registries: list[DeviceRegistry]

    def devices_for_tag(self, tag: str) -> list[str]:
        seen: set[str] = set()
        result: list[str] = []
        for reg in self.registries:
            for did in reg.devices_for_tag(tag):
                if did not in seen:
                    seen.add(did)
                    result.append(did)
        return result

    def devices_for_site(self, site_id: str) -> list[str]:
        seen: set[str] = set()
        result: list[str] = []
        for reg in self.registries:
            for did in reg.devices_for_site(site_id):
                if did not in seen:
                    seen.add(did)
                    result.append(did)
        return result


# ---------------------------------------------------------------------------
# ScopeResolver
# ---------------------------------------------------------------------------

class ScopeResolver:
    """
    Translates a Scope into a deduplicated, ordered list of device IDs.

    The resolver is stateless beyond its registry reference — safe to share
    across threads and requests.

    Parameters
    ----------
    registry : Any object satisfying the DeviceRegistry protocol.
    """

    def __init__(self, registry: DeviceRegistry) -> None:
        self._registry = registry

    def resolve(self, scope: Scope) -> list[str]:
        """
        Return the flat list of device IDs described by *scope*.

        Order is deterministic:
        - device scope      → [device_id]
        - tag scope         → registry order for that tag
        - multi-group scope → registry order per tag, duplicates removed,
                              first-seen ordering preserved across tags
        - site scope        → registry order for that site

        Raises
        ------
        ValueError  if the scope is empty (no field set).
        LookupError if the resolved device list is empty (tag/site unknown or empty).
        """
        if scope.device_id:
            return [scope.device_id]

        if scope.tag:
            ids = self._registry.devices_for_tag(scope.tag)
            if not ids:
                raise LookupError(
                    f"Tag '{scope.tag}' is unknown or has no devices"
                )
            return ids

        if scope.tags:
            seen: set[str] = set()
            result: list[str] = []
            for tag in scope.tags:
                for did in self._registry.devices_for_tag(tag):
                    if did not in seen:
                        seen.add(did)
                        result.append(did)
            if not result:
                raise LookupError(
                    f"None of the tags {list(scope.tags)} resolved to any devices"
                )
            return result

        if scope.site_id:
            ids = self._registry.devices_for_site(scope.site_id)
            if not ids:
                raise LookupError(
                    f"Site '{scope.site_id}' is unknown or has no devices"
                )
            return ids

        raise ValueError("Empty scope")

    def resolve_with_meta(self, scope: Scope) -> dict:
        """
        Resolve and return a dict with the device list plus scope metadata.

        Useful when you need the scope_type / label alongside the IDs.

        Returns
        -------
        {
            "scope_type":  "device" | "group" | "multi_group" | "site",
            "scope_label": str,
            "device_ids":  list[str],
            "device_count": int,
        }
        """
        device_ids = self.resolve(scope)
        return {
            "scope_type":   scope.scope_type,
            "scope_label":  scope.label,
            "device_ids":   device_ids,
            "device_count": len(device_ids),
        }


# ---------------------------------------------------------------------------
# Quick smoke-test (run: python scope_resolver.py)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    registry = InMemoryDeviceRegistry(
        tag_map={
            "HVAC":     ["DEV-001", "DEV-002", "DEV-003"],
            "Floor-A":  ["DEV-001", "DEV-004"],
            "Organic":  ["DEV-005", "DEV-006"],
            "Inorganic":["DEV-007", "DEV-008"],
            "Building-1":["DEV-001","DEV-002","DEV-003","DEV-004","DEV-005",
                          "DEV-006","DEV-007","DEV-008"],
        },
        site_map={
            "SITE-MAIN": ["DEV-001","DEV-002","DEV-003","DEV-004",
                          "DEV-005","DEV-006","DEV-007","DEV-008"],
        },
    )
    resolver = ScopeResolver(registry)

    cases = [
        Scope.device("DEV-001"),
        Scope.group("HVAC"),
        Scope.groups(["HVAC", "Floor-A"]),    # DEV-001 should appear only once
        Scope.groups(["Organic", "Inorganic"]),
        Scope.site("SITE-MAIN"),
    ]

    for scope in cases:
        meta = resolver.resolve_with_meta(scope)
        print(f"[{meta['scope_type']:12s}] {meta['scope_label']:30s} "
              f"→ {meta['device_count']} devices: {meta['device_ids']}")
